package worker

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import scala.collection.mutable.ListBuffer
import java.io.{BufferedOutputStream, File, FileOutputStream}
import services.WorkerState.*
import services.{GrpcNetworkService, Key, NetworkService, StorageService, WorkerID, WorkerState}
import services.RecordOrdering.ordering.compare
import services.Constant
import sorting.sorting._

class WorkerNode(
                  val workerID: WorkerID,
                  val masterAddress: String,
                  val inputDirs: List[String],
                  val outputDir: String
                ) {

  @volatile private var state: WorkerState = Unregistered
  @volatile private var splitters: List[Key] = _
  @volatile private var allWorkerIDs: List[WorkerID] = _

  private var masterClient: SortingServiceGrpc.SortingServiceBlockingStub = _
  private var channel: ManagedChannel = _
  private var networkService: NetworkService = _

  // ... existing code (connectToMaster, start) ...
  private def connectToMaster(): Unit = {
    val Array(host, port) = masterAddress.split(":")
    channel = ManagedChannelBuilder.forAddress(host, port.toInt).usePlaintext().build
    masterClient = SortingServiceGrpc.blockingStub(channel)
  }

  def start(): Unit = {
    connectToMaster()
    try { registerWithMaster() } catch { case _: Exception => }

    while (state != Done) {
      try {
        if (state == Unregistered) registerWithMaster()

        state match {
          case Sampling =>
            executeSampling()
            waitForNextPhase(targetState = Shuffling)

          case Shuffling =>
            // 데이터가 없으면 Fetch 요청
            if (splitters == null || allWorkerIDs == null) {
              fetchGlobalState()
            }

            // Fetch 후에도 없으면(마스터가 아직 준비 안됨) Heartbeat 대기
            if (splitters != null && allWorkerIDs != null) {
              executeShuffleAndPartition()
              masterClient.notifyShuffleComplete(NotifyRequest(workerId = workerID))
              waitForNextPhase(targetState = Merging)
            } else {
              Thread.sleep(1000)
              pollHeartbeat()
            }

          case Merging =>
            executeMerge()
            masterClient.notifyMergeComplete(NotifyRequest(workerId = workerID))
            state = Done

          case _ =>
            Thread.sleep(1000)
            pollHeartbeat()
        }
      } catch {
        case e: Exception =>
          println(s"Worker Error: ${e.getMessage}. Retrying...")
          Thread.sleep(3000)
      }
    }
    if (channel != null) channel.shutdown()
  }

  // ... existing code (registerWithMaster) ...
  private def registerWithMaster(): Unit = {
    val req = RegisterRequest(workerId = workerID)
    val reply = masterClient.registerWorker(req)
    this.state = WorkerState(reply.assignedState)
    // RegisterReply에는 여전히 포함되어 있을 수 있으므로 업데이트
    if (reply.splitters.nonEmpty) updateLocalData(reply.splitters, reply.allWorkerIds)
  }

  // [수정] Heartbeat에서는 상태만 체크
  private def pollHeartbeat(): Unit = {
    val reply = masterClient.heartbeat(HeartbeatRequest(workerId = workerID))
    val masterState = reply.state
    if (masterState.name == "Waiting") return

    val newState = WorkerState(masterState.value)
    if (newState != this.state) {
      this.state = newState
      // 상태가 Shuffling 등으로 바뀌었는데 데이터가 없으면 Fetch
      if (newState == Shuffling && splitters == null) {
        fetchGlobalState()
      }
    }
  }

  // [신규] 별도 RPC로 데이터 받아오기
  private def fetchGlobalState(): Unit = {
    try {
      println(s"[$workerID] Fetching global state (splitters & workers)...")
      val reply = masterClient.getGlobalState(GetGlobalStateRequest(workerId = workerID))
      updateLocalData(reply.splitters, reply.allWorkerIds)
    } catch {
      case e: Exception => println(s"Failed to fetch global state: ${e.getMessage}")
    }
  }

  private def updateLocalData(protoSplitters: Seq[ProtoKey], protoIds: Seq[String]): Unit = {
    if (protoSplitters.nonEmpty) {
      this.splitters = protoSplitters.map(_.key.toByteArray).toList
      println(s"[$workerID] Updated splitters: ${splitters.size} count")
    }
    if (protoIds.nonEmpty) {
      this.allWorkerIDs = protoIds.toList
    }
  }

  private def waitForNextPhase(targetState: WorkerState): Unit = {
    this.state = WorkerState.Waiting
    while (this.state != targetState && this.state != Done) {
      Thread.sleep(1000)
      pollHeartbeat()
    }
  }

  // ... existing code (findPartitionIndex, executeSampling, executeShuffleAndPartition, executeMerge) ...
  private def findPartitionIndex(key: Key): Int = {
    if (splitters == null || splitters.isEmpty) return 0
    var left = 0
    var right = splitters.length - 1
    if (compare(key, splitters.head) <= 0) return 0
    if (compare(key, splitters.last) > 0) return splitters.length
    while (left <= right) {
      val mid = (left + right) / 2
      if (compare(key, splitters(mid)) <= 0) right = mid - 1
      else left = mid + 1
    }
    left
  }

  private def executeSampling(): Unit = {
    val samples = ListBuffer[Key]()
    for (dir <- inputDirs; file <- StorageService.listFiles(dir)) samples ++= StorageService.extractSamples(file)
    masterClient.submitSamples(SampleRequest(workerId = workerID, samples = samples.map(k => ProtoKey(com.google.protobuf.ByteString.copyFrom(k))).toSeq))
    this.state = Shuffling
  }

  private def executeShuffleAndPartition(): Unit = {
    println("Phase: Shuffling")
    val P = Constant.Size.partitionPerWorker
    val totalPartitions = allWorkerIDs.length * P

    val localFiles = Array.ofDim[File](totalPartitions)
    val localStreams = Array.ofDim[BufferedOutputStream](totalPartitions)
    for (i <- 0 until totalPartitions) {
      localFiles(i) = new File(s"worker_${workerID}_to_${allWorkerIDs(i/P)}_p${i%P}")
      localStreams(i) = new BufferedOutputStream(new FileOutputStream(localFiles(i)))
    }

    val recvFile = new File(s"worker_${workerID}_recv_temp")
    val recvBos = new BufferedOutputStream(new FileOutputStream(recvFile))
    networkService = new GrpcNetworkService(allWorkerIDs, workerID)
    networkService.startReceivingData { record => recvBos.synchronized { recvBos.write(record) } }

    try {
      for (dir <- inputDirs; file <- StorageService.listFiles(dir)) {
        val it = StorageService.readRecords(file)
        while (it.hasNext) {
          val record = it.next()
          val pIdx = findPartitionIndex(record.take(Constant.Size.key))
          if (pIdx >= 0 && pIdx < totalPartitions) {
            val targetWorkerID = allWorkerIDs(pIdx / P)
            if (targetWorkerID == workerID) localStreams(pIdx).write(record)
            else networkService.sendData(targetWorkerID, record)
          }
        }
      }
      localStreams.foreach(_.close())
      networkService.finishSending()
      networkService.awaitReceivingCompletion()
      recvBos.close()

      val myStartIdx = allWorkerIDs.indexOf(workerID) * P
      for (i <- myStartIdx until myStartIdx + P) {
        val fos = new BufferedOutputStream(new FileOutputStream(new File(outputDir, s"partition.$i")))
        if (localFiles(i).exists()) {
          val it = StorageService.readRecords(localFiles(i))
          while(it.hasNext) fos.write(it.next())
          localFiles(i).delete()
        }
        val it = StorageService.readRecords(recvFile)
        while(it.hasNext) {
          val rec = it.next()
          if (findPartitionIndex(rec.take(Constant.Size.key)) == i) fos.write(rec)
        }
        fos.close()
      }
      recvFile.delete()
    } finally {
      networkService.shutdown()
    }
  }

  private def executeMerge(): Unit = {}
}