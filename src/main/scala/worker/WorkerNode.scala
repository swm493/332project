package worker

import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}

import scala.concurrent.ExecutionContext
import utils.{Logging, NetworkUtils, PartitionID, Port, WorkerState}
import utils.WorkerState.*
import sorting.common.*
import sorting.master.*
import sorting.worker.*

class WorkerNode(val masterAddress: String, inputDirs: List[String], outputDir: String)(implicit ec: ExecutionContext) {
  private val Array(host, port) = masterAddress.split(":")
  private val channel = ManagedChannelBuilder.forAddress(host, port.toInt).usePlaintext().build
  private val masterClient = MasterServiceGrpc.blockingStub(channel)

  private val context = new WorkerContext(inputDirs, outputDir, masterClient)

  private var server: Server = _

  private val onDataReceived = (partitionID: PartitionID, data: Array[Byte]) => {
    Logging.logInfo(s"Received partition $partitionID data (${data.length} bytes)")
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

    val actualPort: Port = server.getPort
    context.updateSelfPort(actualPort)

    Logging.logEssential(s"Worker server started on ${context.selfAddress}")

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
          Logging.logWarning(s"Failed to register with Master ($masterAddress): ${e.getMessage}")
          Logging.logInfo("Retrying in 5 seconds...")
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
            Logging.logWarning(s"Unknown state: $currentState")
            Thread.sleep(1000)
        }
      } catch {
        case e: Exception =>
          Logging.logSevere(s"Error in loop: ${e.getMessage}")
          e.printStackTrace()
          Thread.sleep(5000)
          currentState = pollHeartbeat()
      }
    }
  }

  private def stop(): Unit = {
    Logging.logInfo("Worker shutting down...")
    if (context != null) context.shutdown()
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
      Logging.logInfo(s"Registered as Worker ID ${context.myEndpoint.id}")
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
      case e: Exception => Logging.logWarning(s"Failed to fetch global state: ${e.getMessage}")
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

  private def toDomain(proto: sorting.common.WorkerEndpoint): utils.WorkerEndpoint = {
    val addr = proto.address.get
    utils.WorkerEndpoint(proto.id.toInt, utils.NodeAddress(addr.ip, addr.port))
  }
}