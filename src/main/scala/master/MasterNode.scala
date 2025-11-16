package master

import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}
import java.util.logging.Logger
import scala.collection.mutable.{Map => MutableMap}

import services.WorkerState._
import services.{Key, WorkerID}
// ScalaPB가 sorting.proto로부터 생성할 코드들
import sorting.sorting._
import com.google.protobuf.ByteString

/**
 * MasterNode는 gRPC 서비스 (SortingService)를 구현합니다.
 * (MasterNode.scala 프로토타입 기반)
 */
class MasterNode(executionContext: ExecutionContext, port: Int, val numWorkers: Int) {

  private val logger = Logger.getLogger(classOf[MasterNode].getName)
  private var server: Server = null

  // --- Master의 상태 관리 ---
  // (MasterNode.scala 프로토타입의 변수들)
  private val workerStatus = MutableMap[WorkerID, WorkerState]()
  private val workerSamples = MutableMap[WorkerID, List[Key]]()
  @volatile private var globalSplitters: List[Key] = null
  @volatile private var allWorkerIDs = List[WorkerID]()

  // gRPC 서비스 구현체
  private object SortingServiceImpl extends SortingServiceGrpc.SortingService {

    // [RPC] 워커가 마스터에 등록 (MasterNode.scala - RegisterWorker)
    override def registerWorker(req: RegisterRequest): Future[RegisterReply] = {
      val workerID = req.workerId
      logger.info(s"Worker $workerID registering...")

      // 동기화 블록으로 상태 관리
      val (assignedState, splittersToSend, workersToSend) = synchronized {
        if (!workerStatus.contains(workerID) || workerStatus(workerID) == Failed) {
          // (신규 등록) 또는 (실패 후 재등록)
          if (!workerStatus.contains(workerID)) {
            allWorkerIDs = workerID :: allWorkerIDs
          }

          // 현재 마스터의 전역 상태에 따라 워커의 다음 상태 결정
          val recoveryState = if (globalSplitters != null) {
            // 샘플링이 이미 끝났으면 셔플 단계로
            workerStatus(workerID) = Shuffling
            Shuffling
          } else {
            // 아직 샘플링 단계면 샘플링부터
            workerStatus(workerID) = Sampling
            Sampling
          }

          logger.info(s"Worker $workerID registered. Assigned state: $recoveryState. Total workers: ${workerStatus.size}/$numWorkers")

          // 모든 워커가 등록되었는지 확인
          if (workerStatus.size == numWorkers && recoveryState == Sampling) {
            logger.info("All workers registered. Broadcasting Sampling phase.")
            // (이 RPC 응답이 가야 워커가 알 수 있으므로 별도 broadcast는 불필요)
          }

          (recoveryState, globalSplitters, allWorkerIDs)

        } else {
          // (중복 등록 - 이미 활성 상태)
          logger.warning(s"Worker $workerID tried to re-register while active.")
          (workerStatus(workerID), globalSplitters, allWorkerIDs)
        }
      }

      // Proto Key로 변환
      val protoSplitters = if (splittersToSend != null) {
        splittersToSend.map(key => ProtoKey(key = ByteString.copyFrom(key)))
      } else {
        Seq.empty
      }

      Future.successful(
        RegisterReply(
          assignedState = assignedState.id,
          splitters = protoSplitters,
          allWorkerIds = workersToSend
        )
      )
    }

    // [RPC] 워커가 샘플 제출 (MasterNode.scala - SubmitSamples)
    override def submitSamples(req: SampleRequest): Future[SampleReply] = {
      val workerID = req.workerId
      val samples = req.samples.map(_.key.toByteArray).toList
      logger.info(s"Received ${samples.length} samples from $workerID.")

      synchronized {
        workerSamples(workerID) = samples
        workerStatus(workerID) = Shuffling // 샘플 냈으니 셔플 대기

        // 모든 워커가 샘플을 제출했는지 확인
        if (workerSamples.size == numWorkers) {
          logger.info("All samples received. Calculating splitters...")
          calculateSplitters()
          logger.info(s"${globalSplitters.length} splitters calculated. Broadcasting Shuffling phase.")
          // (별도 broadcast 대신, 다음 register/heartbeat 시 splitters가 전달됨)
        }
      }

      Future.successful(SampleReply(ack = true))
    }

    // [RPC] 워커가 셔플 완료 보고 (MasterNode.scala - NotifyShuffleComplete)
    override def notifyShuffleComplete(req: NotifyRequest): Future[NotifyReply] = {
      val workerID = req.workerId
      logger.info(s"Worker $workerID reported Shuffle complete.")

      synchronized {
        workerStatus(workerID) = Merging

        // 모든 워커가 셔플을 완료했는지 확인
        if (workerStatus.values.forall(_ == Merging)) {
          logger.info("All workers finished shuffling. Broadcasting Merging phase.")
          // (별도 broadcast 필요 없음)
        }
      }

      Future.successful(NotifyReply(ack = true))
    }

    // [RPC] 워커가 병합 완료 보고 (MasterNode.scala - NotifyMergeComplete)
    override def notifyMergeComplete(req: NotifyRequest): Future[NotifyReply] = {
      val workerID = req.workerId
      logger.info(s"Worker $workerID reported Merge complete.")

      synchronized {
        workerStatus(workerID) = Done

        // 모든 워커가 완료했는지 확인
        if (workerStatus.values.forall(_ == Done)) {
          logger.info("--- All workers done. Distributed sorting complete! ---")
          // (선택적) 마스터 서버 종료
          // stop() 
        }
      }

      Future.successful(NotifyReply(ack = true))
    }
  }

  // (내부) 스플리터 계산 (MasterNode.scala - calculateSplitters)
  private def calculateSplitters(): Unit = {
    // TODO:
    // 1. 모든 샘플(workerSamples.values.flatten) 수집
    // 2. 샘플 정렬 (Key 비교 로직 필요)
    // 3. numWorkers - 1 개의 스플리터 선택

    // (임시 스텁)
    globalSplitters = List(Array.fill[Byte](10)(50)) // '2'

    if (globalSplitters.isEmpty && numWorkers > 1) {
      logger.warning("Calculated 0 splitters, which might be incorrect for >1 workers.")
    }
  }

  // (내부) 워커 실패 감지 (MasterNode.scala - detectWorkerFailure)
  // TODO: gRPC는 연결 기반이므로, 타임아웃/하트비트 메커니즘이 필요.
  def detectWorkerFailure(workerID: WorkerID): Unit = synchronized {
    if(workerStatus.get(workerID).exists(s => s != Done && s != Failed)) {
      logger.severe(s"Worker $workerID FAILED.")
      workerStatus(workerID) = Failed
      // Fault-Tolerance: 이 워커가 재시작(RegisterWorker)하면
      // 현재 마스터 상태(globalSplitters 등)를 받아 작업을 재개합니다.
    }
  }

  def start(): Unit = {
    server = ServerBuilder.forPort(port)
      .addService(SortingServiceGrpc.bindService(SortingServiceImpl, executionContext))
      .build
      .start

    val ip = java.net.InetAddress.getLocalHost.getHostAddress
    logger.info(s"Master server started, listening on $port")

    // (project.sorting.2025.pptx - 명세서)
    println(s"Master listening on: $ip:$port")
    // (워커 IP는 등록 시점에 결정되므로, 여기서는 출력 불가)
    // (대신, RegisterWorker에서 워커 IP 목록(allWorkerIDs)을 워커에게 전달)

    sys.addShutdownHook {
      System.err.println("*** Shutting down gRPC server...")
      this.stop()
      System.err.println("*** Server shut down.")
    }
    server.awaitTermination()
  }

  def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }
}