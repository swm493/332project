package master

import utils.{Logging, WorkerState}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import utils.{Key, WorkerEndpoint, NodeAddress, Constant, ID, IP}
import utils.WorkerState._
import utils.RecordOrdering.ordering
import sorting.common.ProtoWorkerState

class MasterState(numWorkers: Int) {
  private val workerStatus = new ArrayBuffer[WorkerState](numWorkers)
  private val workerSamples = mutable.Map[ID, List[Key]]()
  private val ipToId = mutable.Map[IP, ID]()

  @volatile private var globalSplitters: List[Key] = _

  private var currentPhase: WorkerState = Unregistered

  private val allWorkerEndpoints = new ArrayBuffer[WorkerEndpoint](numWorkers)

  def registerWorker(workerAddress: NodeAddress): (WorkerState, List[Key], WorkerEndpoint, List[WorkerEndpoint]) = synchronized {
    // 1. Worker ID 식별 또는 생성
    val (workerId, isRecovery) = if (ipToId.contains(workerAddress.ip)) {
      val id = ipToId(workerAddress.ip)
      Logging.logInfo(s"Death Worker recovered: ID=$id, IP=${workerAddress.ip}")
      (id, true)
    } else {
      val id = allWorkerEndpoints.size
      ipToId(workerAddress.ip) = id

      // 신규 워커 초기화
      workerStatus.addOne(Waiting)

      val newEndpoint = WorkerEndpoint(id, workerAddress)
      allWorkerEndpoints.addOne(newEndpoint)
      Logging.logInfo(s"New Worker Registered: ID=$id, IP=${workerAddress.ip}")
      (id, false)
    }

    // 2. 포트 정보 갱신 (Dynamic Port Update)
    val currentEndpoint = allWorkerEndpoints(workerId)
    if (currentEndpoint.address.port != workerAddress.port) {
      val updatedEndpoint = currentEndpoint.copy(address = workerAddress)
      allWorkerEndpoints(workerId) = updatedEndpoint
      Logging.logInfo(s"Worker $workerId port updated: ${currentEndpoint.address.port} -> ${workerAddress.port}")
    }

    val myEndpoint = allWorkerEndpoints(workerId)

    // 3. 상태 결정 (Recovery Logic vs New Registration)
    var assignedState: WorkerState = if (isRecovery) {
      currentPhase match {
        case Unregistered =>
          workerStatus(workerId) = Waiting
          Waiting

        case Sampling =>
          workerStatus(workerId) = Sampling
          Sampling

        case Partitioning =>
          workerStatus(workerId) = Partitioning
          Partitioning

        case Shuffling | Merging =>
          Logging.logWarning(s"Worker $workerId recovered during $currentPhase. Assigning PARTITIONING to recover lost data.")
          workerStatus(workerId) = Partitioning
          for (i <- workerStatus.indices if i != workerId) {
            if (workerStatus(i) != Failed) { // 죽은 놈 빼고
              workerStatus(i) = Waiting
            }
          }
          Partitioning

        case _ =>
          workerStatus(workerId)
      }
    } else {
      // 신규 등록은 기본적으로 Waiting
      Waiting
    }

    // 4. 클러스터 시작 트리거 (신규 등록 시 체크)
    if (!isRecovery && allWorkerEndpoints.size == numWorkers && globalSplitters == null) {
      if (workerStatus.forall(_ == Waiting)) {
        Logging.logInfo("Cluster is full and all workers are WAITING. Starting SAMPLING phase.")
        currentPhase = Sampling
        setAllWorkersState(Sampling)
        // 방금 등록된 워커 상태도 Sampling으로 업데이트
        assignedState = Sampling
      }
    }

    if (isRecovery) {
      Logging.logInfo(s"Worker $workerId recovered. Current Global Phase: $currentPhase. Assigned State: $assignedState")
    }

    // 5. Splitter 정보 결정
    val splittersToSend = if (assignedState.id >= Partitioning.id || assignedState == Failed) globalSplitters else null

    (assignedState, splittersToSend, myEndpoint, allWorkerEndpoints.toList)
  }

  def updateSamples(workerId: ID, samples: List[Key]): Unit = synchronized {
    if (!isValidWorker(workerId)) return

    workerSamples(workerId) = samples
    Logging.logInfo(s"Samples received from Worker $workerId. Total collected: ${workerSamples.size}/$numWorkers")
  }

  def handlePhaseComplete(workerId: ID, finishedPhase: ProtoWorkerState): Unit = synchronized {
    if (!isValidWorker(workerId)) return

    // [복구 완료 감지]
    // 전역 상태는 Shuffling/Merging인데, 특정 워커가 Partitioning을 끝냈다면 이는 "복구 완료"를 의미.
    val isRecoveryCompletion = (currentPhase == Shuffling || currentPhase == Merging) &&
      (finishedPhase == ProtoWorkerState.Partitioning)

    workerStatus(workerId) = Waiting
    Logging.logInfo(s"Worker $workerId finished $finishedPhase and is now WAITING. (${countWaitingWorkers()}/$numWorkers)")

    if (isRecoveryCompletion) {
      Logging.logInfo(s"Worker $workerId completed recovery PARTITIONING. Signalling FAILURE to all workers to restart SHUFFLING.")

      if (!workerStatus.contains(Partitioning)) {
        Logging.logInfo("Recovery complete. No workers in Partitioning. Resuming SHUFFLING phase.")
        transitionToNextPhase(ProtoWorkerState.Partitioning)
      } else {
        Logging.logInfo(s"Worker $workerId recovered, but others are still Partitioning. Waiting...")
      }

    } else {
      // 정상 진행: 모든 워커가 Waiting이면 다음 단계로 전이
      if (countWaitingWorkers() == numWorkers) {
        transitionToNextPhase(finishedPhase)
      }
    }
  }

  private def transitionToNextPhase(finishedPhase: ProtoWorkerState): Unit = {
    finishedPhase match {
      case ProtoWorkerState.Sampling =>
        Logging.logInfo("All workers finished Sampling. Calculating Splitters and moving to PARTITIONING.")
        calculateSplitters()
        currentPhase = Partitioning
        setAllWorkersState(Partitioning)

      case ProtoWorkerState.Partitioning =>
        Logging.logInfo("All workers finished Partitioning. Moving to SHUFFLING.")
        currentPhase = Shuffling
        setAllWorkersState(Shuffling)

      case ProtoWorkerState.Shuffling =>
        Logging.logInfo("All workers finished Shuffling. Moving to MERGING.")
        currentPhase = Merging
        setAllWorkersState(Merging)

      case ProtoWorkerState.Merging =>
        Logging.logInfo("All workers finished Merging. Moving to DONE.")
        currentPhase = Done
        setAllWorkersState(Done)

      case _ =>
        Logging.logWarning(s"Unexpected phase transition request from $finishedPhase")
    }
  }

  private def setAllWorkersState(newState: WorkerState): Unit = {
    for (i <- workerStatus.indices) {
      workerStatus(i) = newState
    }
  }

  private def countWaitingWorkers(): Int = {
    workerStatus.count(_ == Waiting)
  }

  def getWorkerStatus(workerId: ID): WorkerState = synchronized {
    if (isValidWorker(workerId)) workerStatus(workerId) else Failed
  }

  def getGlobalStateData: (List[Key], List[WorkerEndpoint]) = synchronized {
    (globalSplitters, allWorkerEndpoints.toList)
  }

  def isShufflingReady: Boolean = globalSplitters != null

  def isAllWorkersFinished: Boolean = {
    workerStatus.size == numWorkers && workerStatus.forall(_ == WorkerState.Done)
  }

  private def isValidWorker(workerId: ID): Boolean = {
    val valid = workerId >= 0 && workerId < workerStatus.size
    if (!valid) {
      Logging.logWarning(s"Ignored request from unknown or invalid Worker ID: $workerId")
    }
    valid
  }

  private def calculateSplitters(): Unit = {
    val totalPartitions = numWorkers * Constant.Size.partitionPerWorker
    val numSplitters = totalPartitions - 1

    val allSamples = workerSamples.values.flatten.toArray
    if (allSamples.isEmpty) {
      Logging.logWarning("No samples received/found. Splitters will be empty.")
      globalSplitters = List.empty
      return
    }

    java.util.Arrays.sort(allSamples, ordering)

    val splitters = ListBuffer[Key]()
    val step = allSamples.length / totalPartitions
    for (i <- 1 to numSplitters) {
      val idx = Math.min(i * step, allSamples.length - 1)
      splitters += allSamples(idx)
    }

    globalSplitters = splitters.toList
    Logging.logInfo(s"Splitters calculated: ${globalSplitters.size} keys generated from ${allSamples.length} samples.")
  }
}