package worker

import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}

import java.util.logging.Logger
import scala.concurrent.ExecutionContext
import services.WorkerState
import services.WorkerState.*
import sorting.common.*
import sorting.master.*
import services.Constant.Ports
import sorting.worker.*

/**
 * WorkerNode는 상태 관리, 서버 수명 주기(Lifecycle), 전체 흐름을 담당합니다.
 * MasterNode와 동일한 패턴으로 리팩토링되었습니다.
 */
class WorkerNode(val masterAddress: String, inputDirs: List[String], outputDir: String)(implicit ec: ExecutionContext) {
  private val logger = Logger.getLogger(classOf[WorkerNode].getName)

  private val selfIP = services.NetworkUtils.findLocalIpAddress()

  private val MasterWorkerID = s"${selfIP}:${Ports.MasterWorkerPort}"
  private val WorkerWorkerID = s"${selfIP}:${Ports.WorkerWorkerPort}"

  private val Array(host, port) = masterAddress.split(":")
  private val channel = ManagedChannelBuilder.forAddress(host, port.toInt).usePlaintext().build
  private val masterClient = MasterServiceGrpc.blockingStub(channel)

  private val context = new WorkerContext(MasterWorkerID, WorkerWorkerID, inputDirs, outputDir, masterClient)

  private var server: Server = _

  private val onDataReceived = (partitionID: Int, data: Array[Byte]) => {
    logger.info(s"Received partition $partitionID data (${data.length} bytes)")
    context.handleReceivedData(partitionID, data)
  }

  private val networkService = new WorkerNetworkService(WorkerWorkerID, onDataReceived)

  private val phases: Map[WorkerState, WorkerPhase] = Map(
    Sampling -> new SamplingPhase(),
    Shuffling -> new ShufflePhase(),
    Merging -> new MergePhase()
  )

  def start(): Unit = {
    context.networkService = this.networkService

    // [리팩토링] 서버 시작 로직이 Node로 이동
    server = ServerBuilder.forPort(Ports.WorkerWorkerPort)
      .addService(WorkerServiceGrpc.bindService(networkService, ec))
      .build
      .start()

    logger.info(s"Worker server started on $WorkerWorkerID")
    logger.info(s"Worker $MasterWorkerID connecting to Master at $masterAddress")

    try {
      runWorkerLoop()
    } finally {
      stop()
    }
  }

  private def runWorkerLoop(): Unit = {
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
            if (s == Shuffling && !context.isReadyForShuffle) {
              fetchGlobalState()
            }

            if (s != Shuffling || context.isReadyForShuffle) {
              val nextExpected = phases(s).execute(context)
              waitForState(nextExpected)
              currentState = nextExpected
            } else {
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
          currentState = pollHeartbeat()
      }
    }
  }

  private def stop(): Unit = {
    logger.info("Worker shutting down...")
    if (server != null) server.shutdown()
    if (channel != null) channel.shutdown()
    if (networkService != null) networkService.closeChannels() // 클라이언트 채널 정리
  }

  private def register(): WorkerState = {
    val res = masterClient.registerWorker(RegisterRequest(MasterWorkerID, WorkerWorkerID))
    updateContextData(res.splitters, res.allWorkerIDs)
    WorkerState(res.assignedState)
  }

  private def pollHeartbeat(): WorkerState = {
    val res = masterClient.heartbeat(HeartbeatRequest(MasterWorkerID))
    WorkerState.withName(res.state.name)
  }

  private def fetchGlobalState(): Unit = {
    try {
      val res = masterClient.getGlobalState(GetGlobalStateRequest(MasterWorkerID))
      updateContextData(res.splitters, res.allWorkerIds)
    } catch {
      case e: Exception => logger.warning(s"Failed to fetch global state: ${e.getMessage}")
    }
  }

  private def waitForState(target: WorkerState): Unit = {
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