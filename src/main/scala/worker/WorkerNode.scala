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
import services.{PartitionID, Port, NetworkUtils}

class WorkerNode(val masterAddress: String, inputDirs: List[String], outputDir: String)(implicit ec: ExecutionContext) {
  private val logger = Logger.getLogger(classOf[WorkerNode].getName)

  private val Array(host, port) = masterAddress.split(":")
  private val channel = ManagedChannelBuilder.forAddress(host, port.toInt).usePlaintext().build
  private val masterClient = MasterServiceGrpc.blockingStub(channel)

  private val context = new WorkerContext(inputDirs, outputDir, masterClient)

  private var server: Server = _

  private val onDataReceived = (partitionID: PartitionID, data: Array[Byte]) => {
    logger.info(s"Received partition $partitionID data (${data.length} bytes)")
    context.handleReceivedData(partitionID, data)
  }

  private val networkService = new WorkerNetworkService(onDataReceived)

  private val phases: Map[WorkerState, WorkerPhase] = Map(
    Sampling -> new SamplingPhase(),
    Shuffling -> new ShufflePhase(),
    Merging -> new MergePhase()
  )

  def start(): Unit = {
    context.networkService = this.networkService

    server = ServerBuilder.forPort(0) // Port 0 = Random Port
      .addService(WorkerServiceGrpc.bindService(networkService, ec))
      .build
      .start()

    val actualPort: Port = server.getPort()
    context.updateSelfPort(actualPort)

    logger.info(s"Worker server started on ${context.selfAddress}")
    logger.info(s"Worker connecting to Master at $masterAddress")

    try {
      runWorkerLoop()
    } finally {
      stop()
    }
  }

  private def runWorkerLoop(): Unit = {
    var currentState = WorkerState.Unregistered

    while (currentState == WorkerState.Unregistered) {
      try {
        currentState = register()
      } catch {
        case e: Exception =>
          logger.warning(s"Failed to register with Master (${masterAddress}): ${e.getMessage}")
          logger.info("Retrying in 5 seconds...")
          try { Thread.sleep(5000) } catch { case _: InterruptedException => }
      }
    }

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
    if (networkService != null) networkService.closeChannels()
  }

  private def register(): WorkerState = {
    val req = RegisterRequest(
      workerAddress = Some(sorting.common.NodeAddress(context.selfAddress.ip, context.selfAddress.port))
    )
    val res = masterClient.registerWorker(req)

    if (res.workerEndpoint.isDefined) {
      context.myEndpoint = toDomain(res.workerEndpoint.get)
      logger.info(s"Registered as Worker ID ${context.myEndpoint.id}")
    }

    updateContextData(res.splitters, res.allWorkerEndpoints)
    WorkerState(res.assignedState)
  }

  private def pollHeartbeat(): WorkerState = {
    val req = HeartbeatRequest(workerEndpoint = Some(NetworkUtils.workerEndpointToProto(context.myEndpoint)))
    val res = masterClient.heartbeat(req)
    WorkerState.withName(res.state.name)
  }

  private def fetchGlobalState(): Unit = {
    try {
      val req = GetGlobalStateRequest(workerEndpoint = Some(NetworkUtils.workerEndpointToProto(context.myEndpoint)))
      val res = masterClient.getGlobalState(req)
      updateContextData(res.splitters, res.allWorkerEndpoints)
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

  private def updateContextData(splitters: Seq[ProtoKey], endpoints: Seq[sorting.common.WorkerEndpoint]): Unit = {
    if (splitters.nonEmpty) {
      context.splitters = splitters.map(_.key.toByteArray).toList
    }
    if (endpoints.nonEmpty) {
      context.allWorkerEndpoints = endpoints.map(toDomain).toList
    }
  }

  private def toDomain(proto: sorting.common.WorkerEndpoint): services.WorkerEndpoint = {
    val addr = proto.address.get
    services.WorkerEndpoint(proto.id.toInt, services.NodeAddress(addr.ip, addr.port))
  }
}