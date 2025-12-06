package worker

import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import scala.concurrent.ExecutionContext
import utils.{Logging, NetworkUtils, PartitionID, Port, WorkerState}
import utils.WorkerState._
import sorting.common._
import sorting.master._
import sorting.worker._
import java.io.File

class WorkerNode(val masterAddress: String, inputDirs: List[String], outputDir: String)(implicit ec: ExecutionContext) {
  private val Array(host, port) = masterAddress.split(":")
  private val channel = ManagedChannelBuilder.forAddress(host, port.toInt).usePlaintext().build
  private val masterClient = MasterServiceGrpc.blockingStub(channel)

  private val context = new WorkerContext(inputDirs, outputDir, masterClient)

  private var server: Server = _

  private val networkService = new WorkerNetworkService(context)

  private val phases: Map[WorkerState, WorkerPhase] = Map(
    Sampling -> new SamplingPhase(),
    Partitioning -> new PartitioningPhase(),
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

    while (currentState != Done) {
      try {
        currentState match {
          case Unregistered =>
            currentState = register()

          case Waiting =>
            Thread.sleep(1000)
            currentState = pollHeartbeat()

          // [수정] Failed 시 Shuffling 복구 로직
          case Failed =>
            Logging.logSevere("Master signalled FAILURE. Rolling back to restart SHUFFLING Phase...")

            // 파일 삭제는 ShufflePhase 진입 시 자동 수행되므로, 여기서는 메모리 상태만 리셋
            resetShuffleData()

            Logging.logInfo("State reset complete. Forcing state to Shuffling...")
            Thread.sleep(2000)

            // 2. Shuffling 단계부터 바로 재시작
            currentState = Shuffling

          case s if phases.contains(s) =>
            if (s == Partitioning && context.splitters.isEmpty) {
              Logging.logInfo("Fetching global state for splitters...")
              fetchGlobalState()
            }
            if (s == Shuffling && !context.isReadyForShuffle) {
              Logging.logInfo("Fetching global state for shuffle peers...")
              fetchGlobalState()
            }

            val canExecute = s match {
              case Partitioning => context.splitters.nonEmpty
              case Shuffling => context.isReadyForShuffle
              case _ => true
            }

            if (canExecute) {
              phases(s).execute(context)
              notifyMasterPhaseComplete(s)
              currentState = Waiting
            } else {
              Thread.sleep(1000)
              currentState = pollHeartbeat()
            }

          case _ =>
            Logging.logWarning(s"Unknown state: $currentState")
            Thread.sleep(1000)
            currentState = pollHeartbeat()
        }
      } catch {
        case e: RuntimeException if e.getMessage.contains("Master signaled Failed") =>
          Logging.logWarning("Detected Failure signal during wait. Rolling back...")
          currentState = Failed

        case e: Exception =>
          Logging.logSevere(s"Error in loop: ${e.getMessage}")
          e.printStackTrace()
          Thread.sleep(5000)
          currentState = pollHeartbeat()
      }
    }
  }

  // 실패 복구 시: Peer 정보만 초기화하여 다시 fetchGlobalState를 유도
  private def resetShuffleData(): Unit = {
    Logging.logInfo("Resetting Shuffle metadata (clearing peer endpoints)...")
    context.allWorkerEndpoints = List.empty
  }

  private def notifyMasterPhaseComplete(state: WorkerState): Unit = {
    val myEndpointProto = NetworkUtils.workerEndpointToProto(context.myEndpoint)

    val protoPhase = state match {
      case Sampling     => ProtoWorkerState.Sampling
      case Partitioning => ProtoWorkerState.Partitioning
      case Shuffling    => ProtoWorkerState.Shuffling
      case Merging      => ProtoWorkerState.Merging
      case _            => ProtoWorkerState.Unregistered
    }

    if (protoPhase != ProtoWorkerState.Unregistered) {
      Logging.logInfo(s"[$state] Local task done. Sending PhaseComplete($protoPhase) to Master...")
      try {
        masterClient.notifyPhaseComplete(PhaseCompleteRequest(
          workerEndpoint = Some(myEndpointProto),
          phase = protoPhase
        ))
        Logging.logInfo(s"[$state] Master acknowledged phase completion.")
      } catch {
        case e: Exception => Logging.logWarning(s"Failed to notify phase complete: ${e.getMessage}")
      }
    }
  }

  private def stop(): Unit = {
    Logging.logInfo("Worker shutting down...")
    if (context != null) context.shutdown()
    if (server != null) server.shutdown()
    if (channel != null) channel.shutdown()
    if (networkService != null) networkService.closeChannels()

    cleanupTemporaryFiles()
    Logging.logInfo("Shutdown complete.")
  }

  private def cleanupTemporaryFiles(): Unit = {
    Logging.logInfo("Cleaning up temporary files (chunks & received data)...")
    val outDir = new File(outputDir)

    if (outDir.exists() && outDir.isDirectory) {
      // temp_chunks 폴더 삭제
      val tempChunksDir = new File(outDir, "temp_chunks")
      if (tempChunksDir.exists()) {
        deleteRecursively(tempChunksDir)
      }

      // partition_received_* 파일 삭제
      utils.FileUtils.listFiles(outDir.getAbsolutePath).foreach { f =>
        if (f.getName.startsWith("partition_received")) {
          f.delete()
        }
      }
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  private def register(): WorkerState = {
    val req = RegisterRequest(
      workerAddress = Some(sorting.common.NodeAddress(context.selfAddress.ip, context.selfAddress.port))
    )
    val res = masterClient.registerWorker(req)

    if (res.workerEndpoint.isDefined) {
      context.myEndpoint = NetworkUtils.workerProtoToEndpoint(res.workerEndpoint.get)
      Logging.logEssential(s"Registered as Worker ID ${context.myEndpoint.id}")
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
      Logging.logInfo(s"[fetchGlobalState] Requesting state for myEndpoint: ${context.myEndpoint}")
      val req = GetGlobalStateRequest(workerEndpoint = Some(NetworkUtils.workerEndpointToProto(context.myEndpoint)))
      val res = masterClient.getGlobalState(req)
      val splittersCount = if (res.splitters != null) res.splitters.size else "null"
      val workersCount = if (res.allWorkerEndpoints != null) res.allWorkerEndpoints.size else "null"
      Logging.logInfo(s"[fetchGlobalState] Received -> Splitters: $splittersCount, Workers: $workersCount")
      updateContextData(res.splitters, res.allWorkerEndpoints)
      Logging.logInfo("[fetchGlobalState] Context updated successfully.")
    } catch {
      case e: Exception => Logging.logWarning(s"Failed to fetch global state: ${e.getMessage}")
    }
  }

  private def updateContextData(splitters: Seq[ProtoKey], endpoints: Seq[sorting.common.WorkerEndpoint]): Unit = {
    if (splitters.nonEmpty) {
      context.splitters = splitters.map(_.key.toByteArray).toList
    }
    if (endpoints.nonEmpty) {
      context.allWorkerEndpoints = endpoints.map(NetworkUtils.workerProtoToEndpoint).toList
    }
  }
}