package master

import utils.{Logging, WorkerState}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

// 프로젝트 의존성
import utils.{Key, WorkerEndpoint, NodeAddress, Constant, ID, IP}
import utils.WorkerState._
import utils.RecordOrdering.ordering

/**
 * Master의 상태 데이터와 비즈니스 로직을 관리하는 클래스입니다.
 * ID, IP 등의 타입 별칭을 적용하여 가독성을 높였습니다.
 */
class MasterState(numWorkers: Int) {
  private val workerStatus = new ArrayBuffer[WorkerState](numWorkers)

  private val workerSamples = new ArrayBuffer[Option[List[Key]]](numWorkers)

  private val ipToId = mutable.Map[IP, ID]()

  private val shuffleReadyWorkers = scala.collection.mutable.Set[ID]()
  private val completedShuffleWorkers = scala.collection.mutable.Set[ID]()

  @volatile private var globalSplitters: List[Key] = _
  private val allWorkerEndpoints = new ArrayBuffer[WorkerEndpoint](numWorkers)

  /**
   * 워커 등록 처리
   */
  def registerWorker(workerAddress: NodeAddress): (WorkerState, List[Key], WorkerEndpoint, List[WorkerEndpoint]) = synchronized {
    val workerId: ID = ipToId.getOrElse(workerAddress.ip, {
      val newId: ID = allWorkerEndpoints.size
      ipToId(workerAddress.ip) = newId

      workerStatus.addOne(Sampling)
      workerSamples.addOne(None)

      val newEndpoint = WorkerEndpoint(newId, workerAddress)
      allWorkerEndpoints.addOne(newEndpoint)

      Logging.logInfo(s"New Worker Registered: ID=$newId, IP=${workerAddress.ip}")
      newId
    })

    // 2. 포트 정보 갱신 (Dynamic Port Update)
    val currentEndpoint = allWorkerEndpoints(workerId)

    if (currentEndpoint.address.port != workerAddress.port) {
      val updatedEndpoint = currentEndpoint.copy(address = workerAddress)
      allWorkerEndpoints(workerId) = updatedEndpoint
      Logging.logInfo(s"Worker $workerId port updated: ${currentEndpoint.address.port} -> ${workerAddress.port}")
    }

    val myEndpoint = allWorkerEndpoints(workerId)

    // 3. 상태 결정 (Recovery Logic)
    val assignedState = if (globalSplitters != null) {
      val savedState = workerStatus(workerId)

      if (savedState.id >= Merging.id) savedState
      else if (savedState.id >= Shuffling.id) Shuffling
      else {
        workerStatus(workerId) = Partitioning
        Partitioning
      }
    } else {
      workerStatus(workerId) = Sampling
      Sampling
    }

    Logging.logInfo(s"Worker $workerId (${workerAddress.ip}) registered/recovered. State: $assignedState. Workers: ${ipToId.size}/$numWorkers")

    val splittersToSend = if (assignedState.id >= Partitioning.id) globalSplitters else null

    (assignedState, splittersToSend, myEndpoint, allWorkerEndpoints.toList)
  }

  def updateSamples(workerId: ID, samples: List[Key]): Boolean = synchronized {
    if (!isValidWorker(workerId)) return false

    workerSamples(workerId) = Some(samples)
    workerStatus(workerId) = Partitioning

    val receivedCount = workerSamples.count(_.isDefined)
    if (receivedCount == numWorkers) {
      Logging.logInfo("All samples received. Calculating splitters...")
      calculateSplitters()
      return true
    }
    false
  }

  def waitForShuffleReady(workerId: ID): Boolean = synchronized {
    if (!isValidWorker(workerId)) return false

    workerStatus(workerId) = Shuffling
    shuffleReadyWorkers.add(workerId)

    Logging.logInfo(s"Worker $workerId finished partitioning & ready for shuffle. (${shuffleReadyWorkers.size}/$numWorkers)")

    if (shuffleReadyWorkers.size == numWorkers) {
      Logging.logInfo("Barrier Reached! Releasing all workers...")
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

    completedShuffleWorkers.add(workerId)
    Logging.logInfo(s"Worker $workerId finished shuffling. (${completedShuffleWorkers.size}/$numWorkers)")

    if (completedShuffleWorkers.size == numWorkers) {
      Logging.logInfo("All workers finished shuffling. Moving cluster to MERGING phase.")

      for (id <- workerStatus.indices) {
        if (workerStatus(id) != Failed) {
          workerStatus(id) = Merging
        }
      }
      true
    } else {
      false
    }
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
      Logging.logWarning(s"Ignored request from unknown or invalid Worker ID: $workerId")
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
    Logging.logInfo(s"Splitters calculated: ${globalSplitters.size} keys generated from ${allSamples.length} samples.")
  }
}