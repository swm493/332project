package master

import scala.concurrent.{ExecutionContext, Future}
import com.google.protobuf.ByteString
import java.util.logging.Logger

// 프로젝트 의존성 (환경에 맞게 import 경로 확인 필요)
import services.{Key, NodeID}
import services.WorkerState._
import sorting.master._
import sorting.common._

/**
 * gRPC 통신 규약만 처리하는 클래스입니다.
 * 실제 데이터 처리 및 상태 관리는 MasterState에 위임합니다.
 * develop 브랜치의 getGlobalState 등의 신규 인터페이스가 통합되었습니다.
 */
class MasterNetworkService(state: MasterState)(implicit ec: ExecutionContext)
  extends MasterServiceGrpc.MasterService {

  private val logger = Logger.getLogger(classOf[MasterNetworkService].getName)

  override def registerWorker(req: RegisterRequest): Future[RegisterReply] = {
    // State에 등록 위임
    val (assignedState, splitters, allIds) = state.registerWorker(req.masterWorkerID, req.workerWorkerID)

    // 응답 객체 생성
    Future.successful(RegisterReply(
      assignedState = assignedState.id,
      splitters = toProtoKeys(splitters),
      allWorkerIDs = allIds
    ))
  }

  override def heartbeat(req: HeartbeatRequest): Future[HeartbeatReply] = {
    val s = state.getWorkerStatus(req.workerId)

    // [Logic Merge] develop 브랜치의 로직 반영
    // Shuffling 단계지만 아직 Splitter가 준비되지 않았다면 Waiting 상태로 응답
    val responseState = s match {
      case Shuffling if !state.isShufflingReady =>
        HeartbeatReply.WorkerHeartState.Waiting
      case _ =>
        HeartbeatReply.WorkerHeartState.fromName(s.toString).getOrElse(HeartbeatReply.WorkerHeartState.Unregistered)
    }

    // Heartbeat는 가볍게 상태만 반환 (데이터 전송 최소화)
    Future.successful(HeartbeatReply(
      state = responseState
    ))
  }

  // [New Feature from develop] 대용량 상태 정보(Splitter 등)를 별도로 요청하는 메서드
  override def getGlobalState(req: GetGlobalStateRequest): Future[GetGlobalStateReply] = {
    val (splitters, allIds) = state.getGlobalStateData

    Future.successful(GetGlobalStateReply(
      splitters = toProtoKeys(splitters),
      allWorkerIds = if (allIds != null) allIds else Seq.empty
    ))
  }

  // Barrier RPC 구현
  override def checkShuffleReady(req: ShuffleReadyRequest): Future[ShuffleReadyReply] = {
    // 여기서 스레드가 블로킹됩니다. (MasterApp의 스레드 풀이 충분하므로 안전)
    val isSuccess = state.waitForShuffleReady(req.workerId)

    Future.successful(ShuffleReadyReply(allReady = isSuccess))
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
    // 1. 상태 업데이트
    state.updateMergeStatus(req.workerId)

    // [수정] 모든 워커가 작업을 완료했는지 확인
    if (state.isAllWorkersFinished) {
      logger.info(s"All workers have completed the job. Initiating shutdown sequence.")
      scheduleShutdown()
    }

    Future.successful(NotifyReply(ack = true))
  }

  /**
   * [추가] 시스템 종료 헬퍼 메서드
   * 마지막 Worker에게 응답(NotifyReply)이 전송될 시간을 확보하기 위해
   * 별도 스레드에서 지연 후 종료합니다.
   */
  private def scheduleShutdown(): Unit = {
    new Thread(() => {
      try {
        // gRPC 응답이 네트워크를 타고 갈 시간을 줌 (예: 1~2초)
        Thread.sleep(2000)
        logger.info("Master shutting down now.")
        System.exit(0) // 프로그램 강제 종료
      } catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
    }).start()
  }

  // Helper: List[Key] -> Seq[ProtoKey] 변환
  private def toProtoKeys(keys: List[Key]): Seq[ProtoKey] = {
    if (keys == null) Seq.empty
    else keys.map(k => ProtoKey(ByteString.copyFrom(k)))
  }
}