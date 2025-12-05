package master

import scala.concurrent.{ExecutionContext, Future}
import com.google.protobuf.ByteString
import utils.{Logging, NetworkUtils}

import utils.{Key, ID}
import utils.WorkerState._
import sorting.master._
import sorting.common._

class MasterNetworkService(state: MasterState)(implicit ec: ExecutionContext)
  extends MasterServiceGrpc.MasterService {

  override def registerWorker(req: RegisterRequest): Future[RegisterReply] = {
    val protoAddress = req.workerAddress.getOrElse(throw new IllegalArgumentException("Worker address is missing"))
    val domainAddress = utils.NodeAddress(protoAddress.ip, protoAddress.port)

    val (assignedState, splitters, myDomainEndpoint, allDomainEndpoints) = state.registerWorker(domainAddress)

    Future.successful(RegisterReply(
      assignedState = assignedState.id,
      workerEndpoint = Some(NetworkUtils.workerEndpointToProto(myDomainEndpoint)),
      splitters = toProtoKeys(splitters),
      allWorkerEndpoints = allDomainEndpoints.map(NetworkUtils.workerEndpointToProto)
    ))
  }

  override def heartbeat(req: HeartbeatRequest): Future[HeartbeatReply] = {
    val domainEndpoint = req.workerEndpoint.map(toDomainEndpoint).getOrElse(throw new IllegalArgumentException("Worker endpoint is missing"))
    val workerId: ID = domainEndpoint.id

    val s = state.getWorkerStatus(workerId)
    val responseState = s match {
      case Partitioning | Shuffling if !state.isShufflingReady => ProtoWorkerState.Waiting
      case _ => ProtoWorkerState.fromName(s.toString).getOrElse(ProtoWorkerState.Unregistered)
    }
    Future.successful(HeartbeatReply(state = responseState))
  }

  override def getGlobalState(req: GetGlobalStateRequest): Future[GetGlobalStateReply] = {
    val (splitters, allDomainEndpoints) = state.getGlobalStateData
    Future.successful(GetGlobalStateReply(
      splitters = toProtoKeys(splitters),
      allWorkerEndpoints = allDomainEndpoints.map(NetworkUtils.workerEndpointToProto)
    ))
  }

  override def submitSamples(req: SampleRequest): Future[SampleReply] = {
    val workerId: ID = req.workerEndpoint.map(toDomainEndpoint).map(_.id).getOrElse(-1)
    val samples = req.samples.map(_.key.toByteArray).toList
    if (workerId != -1) state.updateSamples(workerId, samples)
    Future.successful(SampleReply(ack = true))
  }

  override def notifyPhaseComplete(req: PhaseCompleteRequest): Future[PhaseCompleteReply] = {
    val workerId: ID = req.workerEndpoint.map(toDomainEndpoint).map(_.id).getOrElse(-1)
    val phase = req.phase

    if (workerId != -1) {
      phase match {
        case ProtoWorkerState.Sampling =>
          Logging.logInfo(s"Worker $workerId notified Sampling complete.")

        case ProtoWorkerState.Partitioning =>
          state.waitForShuffleReady(workerId)

        case ProtoWorkerState.Shuffling =>
          state.updateShuffleStatus(workerId)

        case ProtoWorkerState.Merging =>
          state.updateMergeStatus(workerId)
          if (state.isAllWorkersFinished) scheduleShutdown()

        case _ =>
          Logging.logWarning(s"Received unexpected phase completion signal from Worker $workerId: $phase")
      }
    }

    Future.successful(PhaseCompleteReply(ack = true))
  }

  private def scheduleShutdown(): Unit = {
    new Thread(() => {
      try {
        Thread.sleep(2000)
        Logging.logInfo("Master shutting down now.")
        System.exit(0)
      } catch { case e: InterruptedException => e.printStackTrace() }
    }).start()
  }

  private def toProtoKeys(keys: List[Key]): Seq[ProtoKey] = {
    if (keys == null) Seq.empty
    else keys.map(k => ProtoKey(ByteString.copyFrom(k)))
  }

  private def toDomainEndpoint(proto: sorting.common.WorkerEndpoint): utils.WorkerEndpoint = {
    val protoAddress = proto.address.getOrElse(throw new IllegalArgumentException("Address is missing"))
    utils.WorkerEndpoint(
      id = proto.id.toInt,
      address = utils.NodeAddress(protoAddress.ip, protoAddress.port)
    )
  }
}