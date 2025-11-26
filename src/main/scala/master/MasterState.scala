package master

import services.{Key, NodeIp}
import services.WorkerState._
import scala.collection.mutable.{Map => MutableMap}
import java.util.logging.Logger

/**
 * Master의 상태 데이터와 비즈니스 로직(스플리터 계산 등)을 관리하는 클래스입니다.
 * 동기화(Locking) 처리를 이곳으로 격리합니다.
 */
class MasterState(numWorkers: Int) {
  private val logger = Logger.getLogger(classOf[MasterState].getName)

  private val workerStatus = MutableMap[NodeIp, WorkerState]()
  private val workerSamples = MutableMap[NodeIp, List[Key]]()

  // 변경 가능한 상태
  @volatile var globalSplitters: List[Key] = null
  @volatile var allWorkerIDs: List[NodeIp] = List.empty

  /**
   * 워커 등록 처리
   * @return (할당된 상태, 현재 스플리터, 전체 워커 목록)
   */
  def registerWorker(workerID: NodeIp): (WorkerState, List[Key], List[NodeIp]) = synchronized {
    if (!workerStatus.contains(workerID)) {
      allWorkerIDs = workerID :: allWorkerIDs
    }

    // 복구 로직: 이미 진행된 단계가 있다면 그 단계로 안내
    val assignedState = if (globalSplitters != null) {
      workerStatus(workerID) = Shuffling // 혹은 현재 진행 중인 단계
      Shuffling
    } else {
      workerStatus(workerID) = Sampling
      Sampling
    }

    logger.info(s"Worker $workerID registered. State: $assignedState. Workers: ${workerStatus.size}/$numWorkers")

    (assignedState, globalSplitters, allWorkerIDs)
  }

  /**
   * 워커 샘플 제출 처리 및 스플리터 계산 트리거
   */
  def updateSamples(workerID: NodeIp, samples: List[Key]): Boolean = synchronized {
    workerSamples(workerID) = samples
    workerStatus(workerID) = Shuffling

    // 모든 워커의 샘플이 도착했는지 확인
    if (workerSamples.size == numWorkers) {
      logger.info("All samples received. Calculating splitters...")
      calculateSplitters()
      return true // 상태 변화 발생 (Shuffling 시작)
    }
    false
  }

  /**
   * 셔플 완료 보고 처리
   */
  def updateShuffleStatus(workerID: NodeIp): Boolean = synchronized {
    workerStatus(workerID) = Merging
    // 모든 워커가 Merging 상태인지 확인
    workerStatus.values.forall(_ == Merging)
  }

  /**
   * 병합 완료 보고 처리
   */
  def updateMergeStatus(workerID: NodeIp): Boolean = synchronized {
    workerStatus(workerID) = Done
    // 모든 워커가 Done 상태인지 확인
    workerStatus.values.forall(_ == Done)
  }

  /**
   * 상태 조회 (Heartbeat 용)
   */
  def getWorkerStatus(workerID: NodeIp): WorkerState = synchronized {
    workerStatus.getOrElse(workerID, Failed)
  }

  def isShufflingReady: Boolean = globalSplitters != null

  // 내부 로직: 스플리터 계산
  private def calculateSplitters(): Unit = {
    // 실제 정렬 및 피벗 선택 로직 구현
    // 예시: val allSamples = workerSamples.values.flatten.toSeq.sorted ...
    globalSplitters = List(Array.fill[Byte](10)(50)) // Dummy
  }
}