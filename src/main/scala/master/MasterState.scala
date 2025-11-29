package master

import services.WorkerState
import java.util.logging.Logger
import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, ArrayBuffer, Map as MutableMap}

// 프로젝트 의존성
import services.{Key, WorkerEndpoint, NodeAddress, Constant, ID, IP}
import services.WorkerState._
import services.RecordOrdering.ordering

/**
 * Master의 상태 데이터와 비즈니스 로직을 관리하는 클래스입니다.
 * ID, IP 등의 타입 별칭을 적용하여 가독성을 높였습니다.
 */
class MasterState(numWorkers: Int) {
  private val logger = Logger.getLogger(classOf[MasterState].getName)

  // workerStatus(id) : 해당 ID 워커의 상태
  private val workerStatus = new ArrayBuffer[WorkerState](numWorkers)

  // workerSamples(id) : 해당 ID 워커의 샘플
  private val workerSamples = new ArrayBuffer[Option[List[Key]]](numWorkers)

  // 식별자 맵: IP -> Worker ID
  private val ipToId = mutable.Map[IP, ID]()

  private val shuffleReadyWorkers = scala.collection.mutable.Set[ID]()

  @volatile private var globalSplitters: List[Key] = _
  private val allWorkerEndpoints = new ArrayBuffer[WorkerEndpoint](numWorkers)

  /**
   * 워커 등록 처리
   */
  def registerWorker(workerAddress: NodeAddress): (WorkerState, List[Key], WorkerEndpoint, List[WorkerEndpoint]) = synchronized {
    // 1. IP 기반 ID 조회 (복구) 또는 신규 생성
    val workerId: ID = ipToId.getOrElse(workerAddress.ip, {
      // --- 신규 등록 로직 ---
      val newId: ID = allWorkerEndpoints.size
      ipToId(workerAddress.ip) = newId

      workerStatus.addOne(Sampling)
      workerSamples.addOne(None)

      val newEndpoint = WorkerEndpoint(newId, workerAddress)
      allWorkerEndpoints.addOne(newEndpoint)

      logger.info(s"New Worker Registered: ID=$newId, IP=${workerAddress.ip}")
      newId
    })

    // 2. 포트 정보 갱신 (Dynamic Port Update)
    val currentEndpoint = allWorkerEndpoints(workerId)

    if (currentEndpoint.address.port != workerAddress.port) {
      val updatedEndpoint = currentEndpoint.copy(address = workerAddress)
      allWorkerEndpoints(workerId) = updatedEndpoint
      logger.info(s"Worker $workerId port updated: ${currentEndpoint.address.port} -> ${workerAddress.port}")
    }

    val myEndpoint = allWorkerEndpoints(workerId)

    // 3. 상태 결정 (Recovery Logic)
    val assignedState = if (globalSplitters != null) {
      val savedState = workerStatus(workerId)
      if (savedState.id >= Merging.id) savedState else Shuffling
    } else {
      workerStatus(workerId) = Sampling
      Sampling
    }

    logger.info(s"Worker $workerId (${workerAddress.ip}) registered/recovered. State: $assignedState. Workers: ${ipToId.size}/$numWorkers")

    val splittersToSend = if (assignedState.id >= Shuffling.id) globalSplitters else null

    (assignedState, splittersToSend, myEndpoint, allWorkerEndpoints.toList)
  }

  def updateSamples(workerId: ID, samples: List[Key]): Boolean = synchronized {
    if (!isValidWorker(workerId)) return false

    workerSamples(workerId) = Some(samples)
    workerStatus(workerId) = Shuffling

    val receivedCount = workerSamples.count(_.isDefined)
    if (receivedCount == numWorkers) {
      logger.info("All samples received. Calculating splitters...")
      calculateSplitters()
      return true
    }
    false
  }

  def waitForShuffleReady(workerId: ID): Boolean = synchronized {
    if (!isValidWorker(workerId)) return false

    shuffleReadyWorkers.add(workerId)
    logger.info(s"Worker $workerId ready for shuffle. (${shuffleReadyWorkers.size}/$numWorkers)")

    if (shuffleReadyWorkers.size == numWorkers) {
      logger.info("Barrier Reached! Releasing all workers...")
      notifyAll()
      return true
    }

    while (shuffleReadyWorkers.size < numWorkers) {
      try {
        wait()
      } catch {
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          return false
      }
    }
    true
  }

  def updateShuffleStatus(workerId: ID): Boolean = synchronized {
    if (!isValidWorker(workerId)) return false
    workerStatus(workerId) = Merging
    workerStatus.forall(_ == Merging)
  }

  def updateMergeStatus(workerId: ID): Boolean = synchronized {
    if (!isValidWorker(workerId)) return false
    workerStatus(workerId) = Done
    workerStatus.forall(_ == Done)
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
      logger.warning(s"Ignored request from unknown or invalid Worker ID: $workerId")
    }
    valid
  }

  private def calculateSplitters(): Unit = {
    val totalPartitions = numWorkers * Constant.Size.partitionPerWorker
    val numSplitters = totalPartitions - 1

    val allSamples = workerSamples.flatten.flatten.toArray
    java.util.Arrays.sort(allSamples, ordering)

    val splitters = ListBuffer[Key]()
    if (allSamples.nonEmpty) {
      val step = allSamples.length / totalPartitions
      for (i <- 1 to numSplitters) {
        val idx = Math.min(i * step, allSamples.length - 1)
        splitters += allSamples(idx)
      }
    }

    globalSplitters = splitters.toList
    logger.info(s"Splitters calculated: ${globalSplitters.size} keys generated from ${allSamples.length} samples.")
  }
}