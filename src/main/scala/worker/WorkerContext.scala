package worker

import sorting.master.*
import utils.{IP, Key, Logging, NetworkUtils, NodeAddress, PartitionID, Port, WorkerEndpoint}
import utils.RecordOrdering.ordering.compare

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Executors}
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

class WorkerContext(
                     val inputDirs: List[String],
                     val outputDir: String,
                     val masterClient: MasterServiceGrpc.MasterServiceBlockingStub
                   ) {
  // 로컬 IP 탐색
  private val selfIP: IP = NetworkUtils.findLocalIpAddress()

  var selfAddress: NodeAddress = NodeAddress(selfIP, 0)

  var myEndpoint: WorkerEndpoint = _

  @volatile var allWorkerEndpoints: List[WorkerEndpoint] = List.empty
  @volatile var splitters: List[Key] = List.empty

  var networkService: WorkerNetworkService = _

  // CPU 코어 수(4개)만큼의 스레드 풀 생성
  private val executorService = Executors.newFixedThreadPool(4)
  val executionContext: ExecutionContext =
    ExecutionContext.fromExecutorService(executorService)

  // 파티션 ID는 Int (PartitionID)
  @volatile private var dataHandler: (PartitionID, Array[Byte]) => Unit = _

  private val receivedDataQueue = new ConcurrentLinkedQueue[(PartitionID, Array[Byte])]()

  private val finishedShufflePeers = ConcurrentHashMap.newKeySet[Int]()
  private val shuffleLock = new Object()

  def updateSelfPort(port: Port): Unit = {
    selfAddress = NodeAddress(selfIP, port)
  }

  def handleReceivedData(partitionID: PartitionID, data: Array[Byte]): Unit = {
    if (dataHandler != null) {
      dataHandler(partitionID, data)
    } else {
      if (data != null && data.length > 0) {
        receivedDataQueue.add((partitionID, data))
      }
    }
  }

  def pollReceivedData(): Option[(PartitionID, Array[Byte])] = {
    Option(receivedDataQueue.poll())
  }

  def clearReceivedData(): Unit = {
    receivedDataQueue.clear()
  }
  
  def enqueueDirectly(partitionID: PartitionID, data: Array[Byte]): Unit = {
    receivedDataQueue.add((partitionID, data))
  }

  def setCustomDataHandler(handler: (PartitionID, Array[Byte]) => Unit): Unit = {
    this.dataHandler = handler
  }

  def isReadyForShuffle: Boolean = splitters.nonEmpty && allWorkerEndpoints.nonEmpty

  def findPartitionIndex(key: Key): Int = {
    if (splitters.isEmpty) return 0

    var left = 0
    var right = splitters.length - 1

    if (compare(key, splitters.head) <= 0) return 0
    if (compare(key, splitters.last) > 0) return splitters.length

    while (left <= right) {
      val mid = (left + right) / 2
      if (compare(key, splitters(mid)) <= 0) right = mid - 1
      else left = mid + 1
    }
    left
  }

  def markPeerFinished(workerId: Int): Unit = {
    finishedShufflePeers.add(workerId)
    val current = finishedShufflePeers.size()
    val total = allWorkerEndpoints.size

    Logging.logInfo(s"[Shuffle] Worker $workerId finished sending. Progress: $current / $total")
    
    if (current >= total) {
      shuffleLock.synchronized {
        shuffleLock.notifyAll()
      }
    }
  }
  
  def waitForShuffleCompletion(): Unit = {
    shuffleLock.synchronized {
      while (finishedShufflePeers.size() < allWorkerEndpoints.size) {
        Logging.logInfo(s"[Shuffle] Waiting for peers... (${finishedShufflePeers.size()} / ${allWorkerEndpoints.size})")
        shuffleLock.wait(1000)
      }
    }
    Logging.logInfo("[Shuffle] All peers finished. Barrier Broken. Proceeding to Merge.")
  }
  
  def clearShuffleStates(): Unit = {
    finishedShufflePeers.clear()
    clearReceivedData()
  }

  // 마스터로부터 최신 주소록(Global State)을 가져오는 메서드
  def refreshGlobalState(): Unit = {
    try {
      Logging.logInfo("[Context] Refreshing Global State from Master due to connection failure...")
      val req = GetGlobalStateRequest(workerEndpoint = Some(NetworkUtils.workerEndpointToProto(myEndpoint)))
      val res = masterClient.getGlobalState(req)

      if (res.allWorkerEndpoints.nonEmpty) {
        val oldList = allWorkerEndpoints
        allWorkerEndpoints = res.allWorkerEndpoints.map(NetworkUtils.workerProtoToEndpoint).toList
        Logging.logInfo(s"[Context] Global State Refreshed. Workers: ${allWorkerEndpoints.size}")

        // (디버깅용) 바뀐 포트가 있는지 확인
        if (oldList.nonEmpty && oldList.size == allWorkerEndpoints.size) {
          for (i <- oldList.indices) {
            if (oldList(i).address != allWorkerEndpoints(i).address) {
              Logging.logEssential(s"Worker $i Address Changed: ${oldList(i).address} -> ${allWorkerEndpoints(i).address}")
            }
          }
        }
      }
    } catch {
      case e: Exception => Logging.logWarning(s"Failed to refresh global state: ${e.getMessage}")
    }
  }

  def shutdown(): Unit = {
    executorService.shutdown()
  }
}