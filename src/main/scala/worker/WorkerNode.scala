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

  // (worker.scala - start)
  def start(): Unit = {
    println(s"Worker $workerID starting... (Master: $masterAddress)")

    // (worker.scala - (재)시작 루프)
    while (state != Done) {
      try {
        if (masterClient == null || channel.isShutdown || channel.isTerminated) {
          connectToMaster()
        }

        // 마스터에 등록하고 현재 내가 해야 할 작업(Phase)을 받음
        // (worker.scala - master.RegisterWorker)
        val req = RegisterRequest(workerId = workerID)
        val reply = masterClient.registerWorker(req)

        this.state = WorkerState(reply.assignedState)
        if (reply.splitters.nonEmpty) {
          this.splitters = reply.splitters.map(_.key.toByteArray).toList
        }
        if (reply.allWorkerIds.nonEmpty) {
          this.allWorkerIDs = reply.allWorkerIds.toList
        }

        println(s"Master assigned state: $state")

        // 마스터가 지정한 단계부터 실행
        if (state == Sampling) {
          executeSampling()
          // 샘플 제출 후, 상태는 마스터가 다시 알려줄 때까지 대기
        }
        else if (state == Shuffling) {
          if (splitters == null || allWorkerIDs == null) {
            println("Waiting for splitters/worker list from master...")
            Thread.sleep(1000) // (worker.scala - 예외 처리)
          } else {
            executeShuffleAndPartition()
            // 셔플 완료 보고
            masterClient.notifyShuffleComplete(NotifyRequest(workerId = workerID, message = "Shuffle complete"))
            println("Shuffle complete. Notified master.")
          }
        }
        else if (state == Merging) {
          executeMerge()
          // 병합 완료 보고
          masterClient.notifyMergeComplete(NotifyRequest(workerId = workerID, message = "Merge complete"))
          println("Merge complete. Notified master.")
          this.state = Done // (master.scala - NotifyMergeComplete)
        }
        else if (state == Done) {
          // 루프 종료
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

    println(s"--- Worker $workerID task finished. ---")
    if (channel != null) channel.shutdown()
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
    // StorageService.mergeSortFiles(receivedFiles, finalOutputFile)

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