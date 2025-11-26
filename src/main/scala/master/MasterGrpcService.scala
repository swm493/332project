package master

import scala.concurrent.{ExecutionContext, Future}
import services.{Key, NodeIp}
import services.WorkerState._
import sorting.sorting._
import com.google.protobuf.ByteString

/**
 * gRPC 통신 규약만 처리하는 클래스입니다.
 * 실제 로직은 MasterState에 위임합니다.
 */
class MasterGrpcService(state: MasterState)(implicit ec: ExecutionContext)
  extends SortingServiceGrpc.SortingService {

  override def registerWorker(req: RegisterRequest): Future[RegisterReply] = {
    val (assignedState, splitters, allIds) = state.registerWorker(req.workerId)

    Future.successful(RegisterReply(
      assignedState = assignedState.id,
      splitters = toProtoKeys(splitters),
      allWorkerIds = allIds
    ))
  }

  override def heartbeat(req: HeartbeatRequest): Future[HeartbeatReply] = {
    val s = state.getWorkerStatus(req.workerId)

    // 상태별 응답 데이터 구성 로직 (View Logic)
    val (responseState, splitters, ids) = s match {
      case Shuffling if !state.isShufflingReady =>
        (HeartbeatReply.WorkerState.Waiting, null, null)
      case Shuffling =>
        (HeartbeatReply.WorkerState.Shuffling, state.globalSplitters, state.allWorkerIDs)
      case _ =>
        (HeartbeatReply.WorkerState.fromName(s.toString).get, null, null)
    }

    Future.successful(HeartbeatReply(
      state = responseState,
      splitters = toProtoKeys(splitters),
      allWorkerIds = if (ids != null) ids else Seq.empty
    ))
  }

  override def submitSamples(req: SampleRequest): Future[SampleReply] = {
    val samples = req.samples.map(_.key.toByteArray).toList
    state.updateSamples(req.workerId, samples)
    Future.successful(SampleReply(ack = true))
  }

  override def notifyShuffleComplete(req: NotifyRequest): Future[NotifyReply] = {
    state.updateShuffleStatus(req.workerId)
    Future.successful(NotifyReply(ack = true))
  }

  override def notifyMergeComplete(req: NotifyRequest): Future[NotifyReply] = {
    state.updateMergeStatus(req.workerId)
    Future.successful(NotifyReply(ack = true))
  }

  // 유틸리티
  private def toProtoKeys(keys: List[Key]): Seq[ProtoKey] = {
    if (keys == null) Seq.empty
    else keys.map(k => ProtoKey(ByteString.copyFrom(k)))
  }
}