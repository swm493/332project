package worker

import io.grpc.ManagedChannelBuilder
import java.util.logging.Logger
import services.{NodeID, WorkerState}
import services.WorkerState._
import sorting.common._
import sorting.worker._
import sorting.master._
import services.Constant.Ports

/**
 * WorkerNode는 상태 관리와 전체 수명 주기를 담당합니다. (Orchestrator)
 * 구체적인 작업 로직은 WorkerPhase 구현체들에게 위임합니다.
 */
class WorkerNode(val masterAddress: String, inputDirs: List[String], outputDir: String) {
  private val logger = Logger.getLogger(classOf[WorkerNode].getName)

  private val selfIP = java.net.InetAddress.getLocalHost.getHostAddress

  private val MasterWorkerID = s"${selfIP}${Ports.MasterWorkerPort}"
  private val WorkerWorkerID = s"${selfIP}${Ports.WorkerWorkerPort}"

  // 1. Master 연결 설정
  private val Array(host, port) = masterAddress.split(":")
  private val channel = ManagedChannelBuilder.forAddress(host, port.toInt).usePlaintext().build
  private val masterClient = MasterServiceGrpc.blockingStub(channel)

  // 2. 컨텍스트 생성 (Phases 간 공유될 데이터 및 헬퍼)
  private val context = new WorkerContext(MasterWorkerID, WorkerWorkerID, inputDirs, outputDir, masterClient)

  // 3. 상태별 실행기(Strategy) 매핑
  private val phases: Map[WorkerState, WorkerPhase] = Map(
    Sampling -> new SamplingPhase(),
    Shuffling -> new ShufflePhase(),
    Merging -> new MergePhase()
  )

  def start(): Unit = {

    logger.info(s"Worker $MasterWorkerID starting...")
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
            // [Logic Merge] Shuffling 단계인데 Splitter 정보가 없으면 받아옴 (develop 로직)
            if (s == Shuffling && !context.isReadyForShuffle) {
              fetchGlobalState()
            }

            // 데이터가 준비되었으면 로직 실행
            if (s != Shuffling || context.isReadyForShuffle) {
              val nextExpected = phases(s).execute(context)
              waitForState(nextExpected)
              currentState = nextExpected
            } else {
              // 준비 안됐으면 대기
              Thread.sleep(1000)
              currentState = pollHeartbeat()
            }

          case _ =>
            logger.warning(s"Unknown state: $currentState")
            Thread.sleep(1000)
        }
      } catch {
        case e: Exception =>
          logger.severe(s"Error in loop: ${e.getMessage}")
          e.printStackTrace()
          Thread.sleep(5000)
          // 에러 발생 시 재등록 시도 혹은 대기
          currentState = pollHeartbeat()
      }
    }

    channel.shutdown()
  }

  private def register(): WorkerState = {
    val res = masterClient.registerWorker(RegisterRequest(MasterWorkerID, WorkerWorkerID))
    updateContextData(res.splitters, res.allWorkerIDs)
    WorkerState(res.assignedState)
  }

  private def pollHeartbeat(): WorkerState = {
    val res = masterClient.heartbeat(HeartbeatRequest(MasterWorkerID))
    // Heartbeat는 가볍게 상태만 체크, 데이터가 필요하면 fetchGlobalState 사용
    val serverState = WorkerState.withName(res.state.name)
    serverState
  }

  // [New Feature from develop] 대용량 메타데이터 별도 요청
  private def fetchGlobalState(): Unit = {
    try {
      logger.info("Fetching global state from master...")
      val res = masterClient.getGlobalState(GetGlobalStateRequest(MasterWorkerID))
      updateContextData(res.splitters, res.allWorkerIds)
    } catch {
      case e: Exception => logger.warning(s"Failed to fetch global state: ${e.getMessage}")
    }
  }

  private def waitForState(target: WorkerState): Unit = {
    // 이미 타겟 상태거나 완료된 상태면 즉시 리턴
    var s = pollHeartbeat()
    while(s != target && s != Done && s != Failed) {
      Thread.sleep(1000)
      s = pollHeartbeat()
    }
  }

  private def updateContextData(splitters: Seq[ProtoKey], ids: Seq[String]): Unit = {
    if (splitters.nonEmpty) context.splitters = splitters.map(_.key.toByteArray).toList
    if (ids.nonEmpty) context.allWorkerIDs = ids.toList
  }
}