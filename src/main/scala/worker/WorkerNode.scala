package worker

import io.grpc.ManagedChannelBuilder
import services.WorkerState
import sorting.sorting.{HeartbeatRequest, RegisterRequest, SortingServiceGrpc}
import services.WorkerState.*
import worker.*

import java.util.logging.Logger

class WorkerNode(val workerID: String, masterAddress: String, inputDirs: List[String], outputDir: String) {

  private val logger = Logger.getLogger(classOf[WorkerNode].getName)

  // 1. 연결 설정
  private val Array(host, port) = masterAddress.split(":")
  private val channel = ManagedChannelBuilder.forAddress(host, port.toInt).usePlaintext().build
  private val masterClient = SortingServiceGrpc.blockingStub(channel)

  // 2. 컨텍스트 생성 (Phases 간 공유될 데이터)
  private val context = WorkerContext(workerID, inputDirs, outputDir, masterClient)

  // 3. 상태별 실행기(Strategy) 매핑
  private val phases: Map[WorkerState, WorkerPhase] = Map(
    Sampling -> new SamplingPhase(),
    Shuffling -> new ShufflePhase(),
    Merging -> new MergePhase()
  )

  def start(): Unit = {
    logger.info(s"Worker $workerID starting...")
    var currentState = register()

    while (currentState != Done && currentState != Failed) {
      try {
        currentState match {
          case Unregistered =>
            currentState = register()

          case Waiting =>
            Thread.sleep(1000)
            currentState = pollHeartbeat()

          case s if phases.contains(s) =>
            // 해당 단계의 로직 실행 (Phase 클래스에 위임)
            val nextExpected = phases(s).execute(context)

            // 실행 후, 마스터가 다음 단계를 승인할 때까지 대기
            waitForState(nextExpected)
            currentState = nextExpected

          case _ =>
            logger.warning(s"Unknown state: $currentState")
            Thread.sleep(1000)
        }
      } catch {
        case e: Exception =>
          logger.severe(s"Error in loop: ${e.getMessage}")
          Thread.sleep(5000) // 재시도 대기
          currentState = Unregistered // 연결 재수립 시도
      }
    }

    channel.shutdown()
  }

  private def register(): WorkerState = {
    val res = masterClient.registerWorker(RegisterRequest(workerID))
    // 컨텍스트 업데이트 (Splitter 정보 등)
    updateContextData(res.splitters, res.allWorkerIds)
    WorkerState(res.assignedState)
  }

  private def pollHeartbeat(): WorkerState = {
    val res = masterClient.heartbeat(HeartbeatRequest(workerID))
    updateContextData(res.splitters, res.allWorkerIds)
    // Proto Enum -> Service Enum 매핑 로직 필요
    WorkerState.withName(res.state.name)
  }

  private def waitForState(target: WorkerState): Unit = {
    var s = pollHeartbeat()
    while(s != target && s != Done && s != Failed) {
      Thread.sleep(1000)
      s = pollHeartbeat()
    }
  }

  private def updateContextData(splitters: Seq[sorting.sorting.ProtoKey], ids: Seq[String]): Unit = {
    if (splitters.nonEmpty) context.splitters = splitters.map(_.key.toByteArray).toList
    if (ids.nonEmpty) context.allWorkerIDs = ids.toList
  }
}