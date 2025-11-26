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

    val myIndex = allWorkerIDs.indexOf(workerID)
    if (myIndex == -1) {
      throw new IllegalStateException(s"workerID $workerID not found in allWorkerIDs")
    }

    // --- 준비: 파일/스트림 ---
    val localTempFile = new File(outputDir, s"shuffle-local-$myIndex.tmp")
    localTempFile.getParentFile.mkdirs()
    val localBos = new BufferedOutputStream(new FileOutputStream(localTempFile, true), 64 * 1024)

    val recvTempFile = new File(outputDir, s"shuffle-recv-$myIndex.tmp")
    recvTempFile.getParentFile.mkdirs()
    val recvBos = new BufferedOutputStream(new FileOutputStream(recvTempFile, true), 64 * 1024)

    // recvBos는 network 콜백에서 사용되므로 동기화 락 필요
    val recvLock = new Object()

    // 안전하게 여러 번 startReceivingData 호출되지 않게 기존 있으면 shutdown 먼저
    if (networkService != null) {
      try {
        networkService.shutdown()
      } catch {
        case _: Throwable => ()
      }
    }

    // 네트워크 수신 콜백: 들어오는 레코드를 recvTempFile에 append
    networkService = new GrpcNetworkService(allWorkerIDs, workerID)
    networkService.startReceivingData { (record: Array[Byte]) =>
      // 기록은 다른 스레드에서 호출될 수 있으므로 동기화
      recvLock.synchronized {
        try {
          recvBos.write(record)
        } catch {
          case e: Exception =>
            println(s"[Worker $workerID] Error writing received record: ${e.getMessage}")
        }
      }
    }

    // 송신 중 고유 시퀀스(디버깅/로그 목적)
    val sendCounter = new AtomicInteger(0)

    try {
      // --- 입력 파일 순회 및 분배 ---
      for (dir <- inputDirs) {
        for (file <- StorageService.listFiles(dir)) {
          val it = StorageService.readRecords(file)
          while (it.hasNext) {
            val record = it.next()
            if (record.length < Constant.Size.key) {
              // 안전장치: 키가 부족하면 건너뜀
              println(s"[Worker $workerID] Skipping short record in ${file.getName}")
            } else {
              val key = record.take(Constant.Size.key)
              val target = findTargetWorker(key, splitters)

              if (target == workerID) {
                // 로컬 파티션에 바로 기록
                localBos.write(record)
              } else {
                // 원격으로 전송 (networkService 구현에 의해 비동기 전송)
                try {
                  networkService.sendData(target, record)
                  val cnt = sendCounter.incrementAndGet()
                  if ((cnt & 0xfff) == 0) { // 주기적 로그 (매 4096건)
                    println(s"[Worker $workerID] Sent $cnt records so far...")
                  }
                } catch {
                  case e: Exception =>
                    println(s"[Worker $workerID] Failed to send to $target: ${e.getMessage}")
                  // 전송 실패 정책: 현재는 로깅 후 계속 (필요시 재시도 로직 추가)
                }
              }
            }
          }
        }
      }

      // --- 송신 종료 및 수신 완료 대기 ---
      localBos.flush()

      // 네트워크 서비스에 송신이 끝났음을 알림
      networkService.finishSending()

      // 수신이 끝날 때까지 대기 (GrpcNetworkService가 내부적으로 완료 플래그를 세움)
      networkService.awaitReceivingCompletion()

      // 수신 스트림 플러시/닫기 (동기화)
      recvLock.synchronized {
        recvBos.flush()
        recvBos.close()
      }
      localBos.close()

      println(s"[Worker $workerID] Shuffle + Partition complete. Sent ${sendCounter.get()} records.")
    } catch {
      case e: Exception =>
        // 안전하게 닫기
        try {
          localBos.close()
        } catch {
          case _: Throwable => ()
        }
        try {
          recvLock.synchronized {
            recvBos.close()
          }
        } catch {
          case _: Throwable => ()
        }
        throw e
    } finally {
      // 네트워크 서비스는 셧다운(혹은 이후 단계에서 재사용 불가하면 shutdown)
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