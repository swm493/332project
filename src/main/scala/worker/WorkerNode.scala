package worker

import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import scala.collection.mutable.ListBuffer
import java.io.File
import scala.util.{Failure, Success, Try}
import services.WorkerState.*
import services.{GrpcNetworkService, Key, NetworkService, StorageService, WorkerID, WorkerState}
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

  // (worker.scala - executeShuffleAndPartition)
  private def executeShuffleAndPartition(): Unit = {
    println("Phase: Shuffling")

    // 1. 워커 간 네트워크 서비스 시작 (gRPC 서버 시작)
    // (worker.scala - startReceivingData)
    networkService = new GrpcNetworkService(allWorkerIDs, workerID)
    networkService.startReceivingData(onDataReceived = (record: Array[Byte]) => {
      // TODO: 받은 레코드를 outputDir의 임시 파일에 저장
      // (이 콜백은 다른 스레드에서 실행될 수 있음)
      // println("Received shuffle data (stub)")
    })

    // 2. 모든 입력 파일을 다시 읽음
    // (worker.scala - readRecords)
    for (dir <- inputDirs) {
      for (file <- StorageService.listFiles(dir)) {
        for (record <- StorageService.readRecords(file)) { // 100바이트 레코드
          val key = record.take(10) // 10바이트 키

          // 3. 키에 맞는 워커 찾기
          val targetWorkerID = findTargetWorker(key, splitters)

          if (targetWorkerID == workerID) {
            // TODO: 자신의 데이터는 로컬 임시 파일에 바로 저장
          } else {
            // 4. 키에 맞는 워커에게 데이터 전송 (Shuffle)
            // (worker.scala - sendDataTo)
            networkService.sendData(targetWorkerID, record)
          }
        }
      }
    }

    // 5. 데이터 전송 완료 신호 및 수신 대기
    // (worker.scala - finishSendingData / waitForReceivingData)
    networkService.finishSending()
    networkService.awaitReceivingCompletion()
    networkService.shutdown() // 셔플용 네트워크 종료
  }

  // (worker.scala - executeMerge)
  private def executeMerge(): Unit = {
    println("Phase: Merging")

    // 1. 셔플 단계에서 받은 모든 임시 파일 목록을 가져옴
    val receivedFiles = StorageService.getReceivedTempFiles(outputDir)

    // 2. K-way merge 수행
    // (project.sorting.2025.pptx - "partition.<n>")
    val finalOutputFile = new File(outputDir, "partition.0") // TODO: 파티션 번호 관리
    StorageService.mergeSortFiles(receivedFiles, finalOutputFile)

    // 3. 임시 파일 삭제
    StorageService.deleteTempFiles(receivedFiles)
  }

  // (내부) 키를 기반으로 타겟 워커 ID를 찾습니다.
  private def findTargetWorker(key: Key, splitters: List[Key]): WorkerID = {
    // TODO:
    // 1. splitters (정렬되어 있음)와 key 비교
    // 2. key가 속하는 범위(파티션) 찾기
    // 3. 해당 파티션을 담당하는 workerID 반환 (allWorkerIDs 리스트의 인덱스)

    // (임시 스텁: 첫 번째 워커에게 모두 전송)
    allWorkerIDs.head
  }
}