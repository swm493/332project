package master

import java.util.logging.Logger
import scala.collection.mutable.{ListBuffer, Map => MutableMap}

// 프로젝트 의존성
import services.{Key, NodeIp, Constant}
import services.WorkerState._
import services.RecordOrdering.ordering // 정렬 기준

/**
 * Master의 상태 데이터와 비즈니스 로직(스플리터 계산 등)을 관리하는 클래스입니다.
 * 모든 동기화(synchronized) 처리는 이곳에서 담당합니다.
 * develop 브랜치의 핵심 로직(calculateSplitters)이 포함되어 있습니다.
 */
class MasterState(numWorkers: Int) {
  private val logger = Logger.getLogger(classOf[MasterState].getName)

  // 워커 상태 및 데이터 저장소
  private val workerStatus = MutableMap[NodeIp, WorkerState]()
  private val workerSamples = MutableMap[NodeIp, List[Key]]()

  // 공유 상태 (Volatile로 가시성 확보)
  @volatile var globalSplitters: List[Key] = null
  @volatile var allNodeIps: List[NodeIp] = List.empty

  /**
   * 워커 등록 처리
   * 기존 상태가 있다면 복구(Recovery) 로직을 수행합니다.
   */
  def registerWorker(NodeIp: NodeIp): (WorkerState, List[Key], List[NodeIp]) = synchronized {
    if (!workerStatus.contains(NodeIp)) {
      allNodeIps = NodeIp :: allNodeIps
    }

    // 복구 로직: 이미 글로벌 상태(Splitter)가 생성되어 있다면 해당 단계로 진입
    val assignedState = if (globalSplitters != null) {
      val savedState = workerStatus.getOrElse(NodeIp, Shuffling)
      // 이미 Merging 이상 진행 중이라면 상태 유지, 아니라면 Shuffling 부터 시작
      if (savedState.id >= Merging.id) savedState else Shuffling
    } else {
      workerStatus(NodeIp) = Sampling
      Sampling
    }

    logger.info(s"Worker $NodeIp registered. State: $assignedState. Workers: ${workerStatus.size}/$numWorkers")

    // 현재 진행 단계에 맞춰 Splitter 제공 여부 결정
    val splittersToSend = if (assignedState.id >= Shuffling.id) globalSplitters else null
    (assignedState, splittersToSend, allNodeIps)
  }

  /**
   * 샘플 제출 처리
   * 모든 워커의 샘플이 모이면 즉시 Splitter를 계산합니다.
   */
  def updateSamples(NodeIp: NodeIp, samples: List[Key]): Boolean = synchronized {
    workerSamples(NodeIp) = samples
    workerStatus(NodeIp) = Shuffling

    // 모든 워커의 샘플이 도착했는지 확인
    if (workerSamples.size == numWorkers) {
      logger.info("All samples received. Calculating splitters...")
      calculateSplitters() // develop 브랜치의 로직 수행
      return true
    }
    false
  }

  /**
   * 셔플 완료 상태 업데이트
   */
  def updateShuffleStatus(NodeIp: NodeIp): Boolean = synchronized {
    workerStatus(NodeIp) = Merging
    // 모든 워커가 Merging 상태인지 확인 (다음 단계 트리거용)
    workerStatus.values.forall(_ == Merging)
  }

  /**
   * 병합 완료 상태 업데이트
   */
  def updateMergeStatus(NodeIp: NodeIp): Boolean = synchronized {
    workerStatus(NodeIp) = Done
    // 모든 워커가 Done 상태인지 확인
    workerStatus.values.forall(_ == Done)
  }

  /**
   * 현재 워커 상태 조회 (Heartbeat용)
   */
  def getWorkerStatus(NodeIp: NodeIp): WorkerState = synchronized {
    workerStatus.getOrElse(NodeIp, Failed)
  }

  /**
   * 글로벌 상태 데이터 조회 (GetGlobalState용)
   */
  def getGlobalStateData: (List[Key], List[NodeIp]) = synchronized {
    (globalSplitters, allNodeIps)
  }

  /**
   * Shuffling 단계 진입 가능 여부 (Splitter 계산 완료 여부)
   */
  def isShufflingReady: Boolean = globalSplitters != null

  /**
   * [Logic Merge] develop 브랜치의 실제 Splitter 계산 로직
   * 수집된 샘플을 정렬하고 파티션 경계값(Splitter)을 추출합니다.
   */
  private def calculateSplitters(): Unit = {
    // 1. 전체 파티션 수 계산
    val totalPartitions = numWorkers * Constant.Size.partitionPerWorker
    val numSplitters = totalPartitions - 1

    // 2. 모든 워커의 샘플을 하나로 합치고 정렬
    // (Array 정렬이 성능상 유리할 수 있음)
    val allSamples = workerSamples.values.flatten.toArray
    java.util.Arrays.sort(allSamples, ordering)

    // 3. 등간격으로 Splitter 추출
    val splitters = ListBuffer[Key]()
    val step = allSamples.length / totalPartitions

    for (i <- 1 to numSplitters) {
      // 인덱스 경계 체크
      val idx = Math.min(i * step, allSamples.length - 1)
      splitters += allSamples(idx)
    }

    globalSplitters = splitters.toList
    logger.info(s"Splitters calculated: ${globalSplitters.size} keys generated from ${allSamples.length} samples.")
  }
}