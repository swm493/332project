package master

import scala.concurrent.{ExecutionContext, Future}
import com.google.protobuf.ByteString
import utils.{Logging, NetworkUtils}

import utils.{Key, ID}
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

    val responseState = ProtoWorkerState.fromName(s.toString).getOrElse(ProtoWorkerState.Unregistered)

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
      state.handlePhaseComplete(workerId, phase)

      // Merging이 끝나고 Done 상태가 되어 셧다운이 필요한지 체크
      if (state.isAllWorkersFinished) {
        scheduleShutdown()
      }
    }

    Future.successful(PhaseCompleteReply(ack = true))
  }

  private def scheduleShutdown(): Unit = {
    new Thread(() => {
      try {
        Thread.sleep(3000) // 모든 워커가 마지막 Heartbeat(Done)를 받을 시간을 줌
        Logging.logInfo("All tasks completed. Master shutting down now.")
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