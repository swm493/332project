package worker

import sorting.master.*
import utils.{IP, Key, Logging, NetworkUtils, NodeAddress, PartitionID, Port, WorkerEndpoint, Constant}
import utils.RecordOrdering.ordering.compare

import java.util.concurrent.{ConcurrentLinkedQueue, Executors}
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

class WorkerContext(
                     val inputDirs: List[String],
                     val outputDir: String,
                     val masterClient: MasterServiceGrpc.MasterServiceBlockingStub
                   ) {
  // 로컬 IP 탐색
  private val selfIP: IP = NetworkUtils.findLocalIpAddress()

  var selfAddress: NodeAddress = NodeAddress(selfIP, 0)

  var myEndpoint: WorkerEndpoint = _

  @volatile var allWorkerEndpoints: List[WorkerEndpoint] = List.empty
  @volatile var splitters: List[Key] = List.empty

  var networkService: WorkerNetworkService = _

  // CPU 코어 수(4개)만큼의 스레드 풀 생성
  private val executorService = Executors.newFixedThreadPool(4)
  val executionContext: ExecutionContext =
    ExecutionContext.fromExecutorService(executorService)

  // 파티션 ID는 Int (PartitionID)
  @volatile private var dataHandler: (PartitionID, Array[Byte]) => Unit = _

  private val receivedDataQueue = new ConcurrentLinkedQueue[(PartitionID, Array[Byte])]()

  private var partitionSignalCounts: Array[Int] = _
  private var finishedPartitions: Array[Boolean] = _
  private val lock = new Object()

  def initShuffleState(): Unit = {
    val P = Constant.Size.partitionPerWorker
    val totalPartitions = allWorkerEndpoints.length * P
    partitionSignalCounts = new Array[Int](totalPartitions)
    finishedPartitions = new Array[Boolean](totalPartitions)
  }

  def updateSelfPort(port: Port): Unit = {
    selfAddress = NodeAddress(selfIP, port)
  }

  def handleReceivedData(partitionID: PartitionID, data: Array[Byte]): Unit = {
    if (partitionID < 0) {
      val realID = -(partitionID) - 1
      lock.synchronized {
        if (partitionSignalCounts != null && realID < partitionSignalCounts.length) {
          partitionSignalCounts(realID) += 1

          // 모든 워커(나 자신 포함)로부터 완료 신호를 받았다면 해당 파티션 완료 처리
          // allWorkerEndpoints.length는 총 워커 수
          if (partitionSignalCounts(realID) == allWorkerEndpoints.length) {
            finishedPartitions(realID) = true
            Logging.logInfo(s"[Shuffle] Partition $realID is fully received from all workers.")
          }
        }
      }
    } else {
      // 일반 데이터 처리
      if (dataHandler != null) {
        dataHandler(partitionID, data)
      } else {
        if (data != null && data.length > 0) {
          receivedDataQueue.add((partitionID, data))
        }
      }
    }
  }

  def isShuffleReceiveComplete(myStartIdx: Int, myEndIdx: Int): Boolean = {
    lock.synchronized {
      if (finishedPartitions == null) return false
      (myStartIdx until myEndIdx).forall(idx => finishedPartitions(idx))
    }
  }

  def pollReceivedData(): Option[(PartitionID, Array[Byte])] = {
    Option(receivedDataQueue.poll())
  }

  def setCustomDataHandler(handler: (PartitionID, Array[Byte]) => Unit): Unit = {
    this.dataHandler = handler
  }

  def isReadyForShuffle: Boolean = splitters.nonEmpty && allWorkerEndpoints.nonEmpty

  def findPartitionIndex(key: Key): Int = {
    if (splitters.isEmpty) return 0

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

  // 마스터로부터 최신 주소록(Global State)을 가져오는 메서드
  def refreshGlobalState(): Unit = {
    try {
      Logging.logInfo("[Context] Refreshing Global State from Master due to connection failure...")
      val req = GetGlobalStateRequest(workerEndpoint = Some(NetworkUtils.workerEndpointToProto(myEndpoint)))
      val res = masterClient.getGlobalState(req)

      if (res.allWorkerEndpoints.nonEmpty) {
        val oldList = allWorkerEndpoints
        allWorkerEndpoints = res.allWorkerEndpoints.map(NetworkUtils.workerProtoToEndpoint).toList
        Logging.logInfo(s"[Context] Global State Refreshed. Workers: ${allWorkerEndpoints.size}")

        // (디버깅용) 바뀐 포트가 있는지 확인
        if (oldList.nonEmpty && oldList.size == allWorkerEndpoints.size) {
          for (i <- oldList.indices) {
            if (oldList(i).address != allWorkerEndpoints(i).address) {
              Logging.logEssential(s"Worker $i Address Changed: ${oldList(i).address} -> ${allWorkerEndpoints(i).address}")
            }
          }
        }
      }
    } catch {
      case e: Exception => Logging.logWarning(s"Failed to refresh global state: ${e.getMessage}")
    }
  }

  def shutdown(): Unit = {
    executorService.shutdown()
  }
}