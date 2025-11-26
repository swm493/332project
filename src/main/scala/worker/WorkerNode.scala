package worker

import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import scala.collection.mutable.ListBuffer
import java.io.File
import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.util.concurrent.atomic.AtomicInteger
import scala.util.{Failure, Success, Try}
import services.WorkerState.*
import services.{GrpcNetworkService, Key, NetworkService, StorageService, WorkerID, WorkerState}
import services.CompareKey.compareKey
import services.Constant
// ScalaPB가 sorting.proto로부터 생성할 코드들
import sorting.sorting._

/**
 * WorkerNode: 실제 워커의 로직을 수행합니다.
 * (worker.scala 프로토타입 기반)
 */
class WorkerNode(
                  val workerID: WorkerID, // 자신의 "IP:Port"
                  val masterAddress: String, // 마스터 "IP:Port"
                  val inputDirs: List[String],
                  val outputDir: String
                ) {

  // --- 워커 상태 관리 ---
  @volatile private var state: WorkerState = Unregistered
  @volatile private var splitters: List[Key] = null
  @volatile private var allWorkerIDs: List[WorkerID] = null

  private var masterClient: SortingServiceGrpc.SortingServiceBlockingStub = null
  private var channel: ManagedChannel = null
  private var networkService: NetworkService = null

  // 마스터 gRPC 클라이언트 연결
  private def connectToMaster(): Unit = {
    val Array(host, port) = masterAddress.split(":")
    channel = ManagedChannelBuilder.forAddress(host, port.toInt)
      .usePlaintext() // (테스트용. 실제 환경에서는 TLS 권장)
      .build
    masterClient = SortingServiceGrpc.blockingStub(channel)
    println(s"Worker $workerID connected to master at $masterAddress")
  }

  def start(): Unit = {
    println(s"Worker $workerID starting... (Master: $masterAddress)")
    connectToMaster()

    try {
      registerWithMaster()
    } catch {
      case e: Exception =>
        println("Initial registration failed. Retrying in loop...")
    }

    while (state != Done) {
      try {
        // 연결 끊김 등의 이유로 Unregistered 상태면 재등록 시도
        if (state == Unregistered) {
          registerWithMaster()
        }

        state match {case Sampling =>
          executeSampling()
          // 샘플 제출 후, 마스터가 Shuffling으로 바꿔줄 때까지 대기해야 함
          // 로컬 상태를 'Waiting' 의미로 변경하거나, 바로 Heartbeat 루프로 진입
          waitForNextPhase(targetState = Shuffling)

        case Shuffling =>
          if (splitters == null || allWorkerIDs == null) {
            // 데이터가 없으면 Heartbeat로 받아옴
            pollHeartbeat()
          } else {
            executeShuffleAndPartition()
            masterClient.notifyShuffleComplete(NotifyRequest(workerId = workerID))
            println("Shuffle complete. Notified master.")
            // 다음 단계(Merging) 대기
            waitForNextPhase(targetState = Merging)
          }

        case Merging =>
          executeMerge()
          masterClient.notifyMergeComplete(NotifyRequest(workerId = workerID))
          println("Merge complete. Notified master.")
          state = Done

        case Done =>
        // Loop 종료

        case _ =>
          // 그 외 상태(Waiting 등)면 잠시 대기 후 Heartbeat
          Thread.sleep(1000)
          pollHeartbeat()
        }

      } catch {
        case e: io.grpc.StatusRuntimeException =>
          // (worker.scala - 실패 에뮬레이션 / 마스터와 연결 끊김)
          println(s"Failed to connect to master ($masterAddress): ${e.getStatus.getDescription}. Retrying in 5 seconds...")
          masterClient = null // 연결 강제 초기화
          Thread.sleep(5000)
        case e: Exception =>
          println(s"An unexpected error occurred: ${e.getMessage}. Retrying in 5 seconds...")
          e.printStackTrace()
          Thread.sleep(5000)
      }
    }

    if (channel != null) channel.shutdown()
  }

  private def registerWithMaster(): Unit = {
    val req = RegisterRequest(workerId = workerID)
    val reply = masterClient.registerWorker(req)

    // Proto Enum -> Local Enum 변환 로직 필요
    // 여기서는 단순화하여 문자열/ID 매핑 가정
    this.state = WorkerState(reply.assignedState)
    updateLocalData(reply.splitters, reply.allWorkerIds)
    println(s"Registered. Initial state: $state")
  }

  private def findTargetWorker(key: Key, splitters: List[Key]): WorkerID = {
    if (allWorkerIDs == null || allWorkerIDs.isEmpty) {
      throw new IllegalStateException("allWorkerIDs is not initialized")
    }

    // 파티션 수 = allWorkerIDs.length
    // splitters.length == partitions - 1 (정상적이면)
    var left = 0
    var right = splitters.length - 1
    var pos = splitters.length // 기본: 마지막 파티션

    while (left <= right) {
      val mid = (left + right) >>> 1
      val cmp = compareKey(key, splitters(mid))
      if (cmp <= 0) {
        pos = mid
        right = mid - 1
      } else {
        left = mid + 1
      }
    }

    // pos는 'key <= splitter(pos)'인 첫 인덱스, 그러므로 파티션 인덱스 = pos
    // 예: pos == 0 => partition 0, pos == splitters.length => partition splitters.length (마지막 파티션)
    val partitionIndex = math.min(pos, allWorkerIDs.length - 1)
    allWorkerIDs(partitionIndex)
  }

  // [보조 함수] Heartbeat로 상태 업데이트
  private def pollHeartbeat(): Unit = {
    val reply = masterClient.heartbeat(HeartbeatRequest(workerId = workerID))
    val masterState = reply.state // Proto Enum

    // Master가 'Waiting'이라고 하면 로컬 상태를 변경하지 않고 리턴
    if (masterState.name == "Waiting") {
      return
    }

    // Master가 새로운 상태(예: Shuffling)를 주면 업데이트
    val newState = WorkerState(masterState.value) // Enum 매핑 필요

    if (newState != this.state) {
      println(s"State updated by Master: ${this.state} -> $newState")
      this.state = newState
      updateLocalData(reply.splitters, reply.allWorkerIds)
    }
  }

  // [보조 함수] 특정 상태가 될 때까지 Heartbeat 반복
  private def waitForNextPhase(targetState: WorkerState): Unit = {
    println(s"Waiting for Master to assign $targetState...")
    this.state = WorkerState.Waiting // 로컬에서 잠시 대기 상태로 둠

    while (this.state != targetState && this.state != Done && this.state != Failed) {
      Thread.sleep(1000)
      pollHeartbeat() // 여기서 this.state가 업데이트됨
    }
  }

  private def updateLocalData(protoSplitters: Seq[ProtoKey], protoIds: Seq[String]): Unit = {
    if (protoSplitters.nonEmpty) {
      this.splitters = protoSplitters.map(_.key.toByteArray).toList
    }
    if (protoIds.nonEmpty) {
      this.allWorkerIDs = protoIds.toList
    }
  }

  // (worker.scala - executeSampling)
  private def executeSampling(): Unit = {
    println("Phase: Sampling")
    val samples = ListBuffer[Key]()
    for (dir <- inputDirs) {
      for (file <- StorageService.listFiles(dir)) {
        // 입력 파일에서 샘플 추출
        samples ++= StorageService.extractSamples(file)
      }
    }

    // 마스터에게 샘플 제출
    val protoSamples = samples.toList.map(key => ProtoKey(key = com.google.protobuf.ByteString.copyFrom(key)))
    masterClient.submitSamples(SampleRequest(workerId = workerID, samples = protoSamples))
    println(s"Submitted ${samples.length} samples to master.")

    // 샘플 제출 후, 셔플 대기 상태로 (로컬 상태)
    this.state = Shuffling
  }

  private def executeShuffleAndPartition(): Unit = {
    println("Phase: Shuffling")

    if (splitters == null || allWorkerIDs == null) {
      throw new IllegalStateException("splitters or allWorkerIDs is null")
    }

    val numWorkers = allWorkerIDs.length
    // P = Common.Size.partitionPerWorker로 가정
    val P = Constant.Size.partitionPerWorker
    val myIndex = allWorkerIDs.indexOf(workerID)
    val totalPartitions = numWorkers * P // 전체 파티션 개수 (N*P)

    if (myIndex == -1) {
      throw new IllegalStateException(s"workerID $workerID not found in allWorkerIDs")
    }

    /** [유틸리티] 파일 생성 */
    def createPartitionFile(fileName: String): File = {
      val file = new File(fileName)
      if (file.getParentFile != null) file.getParentFile.mkdirs()
      file.createNewFile()
      file
    }

    /** [유틸리티] 파일 출력 스트림 생성 (BufferedOutputStream 사용) */
    def createOutputStream(file: File): BufferedOutputStream = {
      new BufferedOutputStream(new FileOutputStream(file), 64 * 1024)
    }

    // tmp (만들거) - N * P
    // (실제 파일로 만들지 않고, 배열 크기만 맞춰서 선언)
    val tmpFiles = Array.ofDim[File](totalPartitions)
    val tmpStreams = Array.ofDim[BufferedOutputStream](totalPartitions)

    // local (보낼거) - N * P
    // Map 출력 데이터를 N*P개의 버퍼에 분배하여 담는 용도로 사용
    val localFiles = Array.ofDim[File](totalPartitions)
    val localStreams = Array.ofDim[BufferedOutputStream](totalPartitions)

    // recv (받을거) - N * P
    // 수신된 데이터를 하나의 임시 파일에 담는 용도로 사용 (배열의 0번째만 활용)
    val recvFiles = Array.ofDim[File](totalPartitions)
    // recv는 네트워킹 스트림으로 관리하므로, 배열은 최소한으로 사용
    var recvBos: BufferedOutputStream = null
    var recvTempFile: File = null

    val recvLock = new Object()

    try {
      // 1. local (보낼거): N*P개의 파일 생성 및 스트림 열기
      // 워커 ID 'i'에게 보낼 파티션 'j'의 파일 (totalPartitionIndex = i*P + j)
      for (i <- 0 until numWorkers) {
        for (j <- 0 until P) {
          val totalPartitionIndex = i * P + j
          localFiles(totalPartitionIndex) = createPartitionFile(
            s"worker_${workerID}_to_worker${allWorkerIDs(i)}_p$j"
          )
          localStreams(totalPartitionIndex) = createOutputStream(localFiles(totalPartitionIndex))
        }
      }

      // 2. recv (받을거): 수신 데이터용 임시 파일 (배열 크기만 N*P로 맞춰주고, 0번째만 사용)
      recvTempFile = createPartitionFile(s"worker_${workerID}_recv_temp")
      recvBos = createOutputStream(recvTempFile)
      recvFiles(0) = recvTempFile // 배열의 0번째에 참조 저장

    } catch {
      case e: Exception =>
        println(s"[Worker $workerID] Error creating files/streams: ${e.getMessage}")
        throw e
    }

    // 기존 네트워크 서비스 종료 (안전하게)
    if (networkService != null) {
      try {
        networkService.shutdown()
      } catch {
        case _: Throwable => ()
      }
    }

    // 네트워크 수신 콜백: 들어오는 레코드를 recvBos에 append
    networkService = new GrpcNetworkService(allWorkerIDs, workerID)
    networkService.startReceivingData { (record: Array[Byte]) =>
      recvLock.synchronized {
        try {
          recvBos.write(record)
        } catch {
          case e: Exception =>
            println(s"[Worker $workerID] Error writing received record: ${e.getMessage}")
        }
      }
    }

    // 키를 기반으로 전체 파티션 인덱스 (0 ~ N*P-1)를 계산하는 보조 함수
    def calculateTotalPartitionIndex(key: Array[Byte], totalPartitions: Int): Int = {
      // 실제 시스템에서는 splitters를 사용하여 정교한 범위 기반 분할을 수행해야 함
      Math.abs(java.util.Arrays.hashCode(key) % totalPartitions)
    }

    // 송신 중 고유 시퀀스
    val sendCounter = new AtomicInteger(0)

    try {
      // ----------------------------------------------------------------------
      // --- 1. 입력 파일 순회 및 분배 (Map 출력) ---
      // ----------------------------------------------------------------------
      for (dir <- inputDirs) {
        for (file <- StorageService.listFiles(dir)) {
          val it = StorageService.readRecords(file)
          while (it.hasNext) {
            val record = it.next()
            if (record.length < Constant.Size.key) {
              println(s"[Worker $workerID] Skipping short record in ${file.getName}")
            } else {
              val key = record.take(Constant.Size.key)
              // 이 레코드가 어느 N*P 파티션으로 가야 하는지 결정 (0 ~ totalPartitions-1)
              val totalPartitionIndex = calculateTotalPartitionIndex(key, totalPartitions)

              // 대상 워커 ID와 로컬 파티션 인덱스 계산
              val targetWorkerIndex = totalPartitionIndex / P
              val targetWorkerID = allWorkerIDs(targetWorkerIndex)

              if (targetWorkerID == workerID) {
                // 1. 로컬에 기록 (local Streams 사용)
                // 이 데이터를 나중에 다른 워커에게 보낼 수도 있으므로, local 배열에 기록.
                localStreams(totalPartitionIndex).write(record)
              } else {
                // 2. 원격으로 전송 (networkService 구현에 의해 비동기 전송)
                // 참고: 실제 시스템에서는 localStreams에 일단 기록 후, 네트워크 서비스가 localFiles의 데이터를 가져가서 전송함.
                // 여기서는 원본 코드의 패턴을 따라 바로 네트워크 전송을 시뮬레이션.
                networkService.sendData(targetWorkerID, record)
                val cnt = sendCounter.incrementAndGet()
                if ((cnt & 0xfff) == 0) {
                  println(s"[Worker $workerID] Sent $cnt records so far...")
                }
              }
            }
          }
        }
      }

      // ----------------------------------------------------------------------
      // --- 2. 송신 종료 및 수신 완료 대기 ---
      // ----------------------------------------------------------------------
      // 모든 local 스트림 플러시 (자신의 Map 출력 버퍼)
      localStreams.foreach(_.flush())
      localStreams.foreach(_.close()) // localFiles 쓰기 완료

      // 네트워크 서비스에 송신이 끝났음을 알림 (다른 워커에게 보낼 데이터가 더 이상 없음)
      networkService.finishSending()

      // 수신이 끝날 때까지 대기
      networkService.awaitReceivingCompletion()

      // 수신 스트림 플러시/닫기
      recvLock.synchronized {
        recvBos.flush()
        recvBos.close()
      }

      // ----------------------------------------------------------------------
      // --- 3. 수신 데이터 재파티션 및 최종 파티션 생성 (tmp 배열 사용) ---
      // ----------------------------------------------------------------------
      println(s"[Worker $workerID] Re-partitioning received data and creating final partitions.")

      // 이 워커가 담당하는 최종 파티션 인덱스 범위: (myIndex * P) 부터 (myIndex * P + P - 1)
      val startIndex = myIndex * P
      val endIndex = startIndex + P

      // 1. tmp 배열에 이 워커의 최종 파티션 파일 생성 및 스트림 열기
      for (i <- startIndex until endIndex) {
        val localPIndex = i - startIndex // 0부터 P-1
        tmpFiles(i) = createPartitionFile(s"worker_${workerID}_final_p$localPIndex")
        tmpStreams(i) = createOutputStream(tmpFiles(i))
      }

      // 2. localFiles에서 자신의 파티션 데이터를 tmpFiles로 복사 (자신의 Map 출력 데이터)
      for (i <- startIndex until endIndex) {
        val localFile = localFiles(i)
        val tmpStream = tmpStreams(i)

        // localFile에서 읽어서 tmpStream에 쓰기
        val it = StorageService.readRecords(localFile)
        while (it.hasNext) {
          tmpStream.write(it.next())
        }
        localFile.delete() // 사용된 local 파일 정리
      }

      // 3. recvTempFile에서 읽어서 자신의 최종 파티션(tmpFiles)으로 분배 및 병합 (다른 워커의 Map 출력 데이터)
      val it = StorageService.readRecords(recvTempFile)
      while (it.hasNext) {
        val record = it.next()
        val key = record.take(Constant.Size.key)
        val totalPartitionIndex = calculateTotalPartitionIndex(key, totalPartitions)

        // 이 레코드가 이 워커가 담당하는 최종 파티션에 속하는지 확인
        if (totalPartitionIndex >= startIndex && totalPartitionIndex < endIndex) {
          tmpStreams(totalPartitionIndex).write(record)
        } else {
          // 이 레코드는 다른 워커에게 잘못 전달되었거나 로직 오류 (발생하면 안 됨)
          println(s"[Worker $workerID] Warning: Received record belongs to partition $totalPartitionIndex, which is not mine.")
        }
      }

      // 임시 파일 정리 및 최종 스트림 닫기
      recvTempFile.delete()
      for (i <- startIndex until endIndex) {
        tmpStreams(i).close()
      }

      println(s"[Worker $workerID] Shuffle + Partition complete. Sent ${sendCounter.get()} records. Created $P final partitions.")
    } catch {
      case e: Exception =>
        // 모든 스트림과 네트워크 안전하게 종료
        // ... (오류 처리 로직 생략)
        throw e
    } finally {
      try {
        networkService.shutdown()
      } catch {
        case _: Throwable => ()
      }
    }
  }

  // (worker.scala - executeMerge)
  private def executeMerge(): Unit = {
    println("Phase: Merging")

    // 1. 셔플 단계에서 받은 모든 임시 파일 목록을 가져옴
    val receivedFiles = StorageService.getReceivedTempFiles(outputDir)

    // 2. K-way merge 수행
    // (project.sorting.2025.pptx - "partition.<n>")
    val finalOutputFile = new File(outputDir, "partition.0") // TODO: 파티션 번호 관리
    // StorageService.mergeSortFiles(receivedFiles, finalOutputFile)

    // 3. 임시 파일 삭제
    StorageService.deleteTempFiles(receivedFiles)
  }
}