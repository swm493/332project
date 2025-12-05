package worker

import sorting.master.*
import utils.{Key, NodeAddress, WorkerEndpoint, IP, Port, PartitionID, NetworkUtils}
import utils.RecordOrdering.ordering.compare

import java.util.concurrent.{ConcurrentLinkedQueue, Executors}
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

  var allWorkerEndpoints: List[WorkerEndpoint] = List.empty
  var splitters: List[Key] = List.empty

  var networkService: WorkerNetworkService = _

  // CPU 코어 수(4개)만큼의 스레드 풀 생성
  private val executorService = Executors.newFixedThreadPool(4)
  val executionContext: ExecutionContext =
    ExecutionContext.fromExecutorService(executorService)

  // 파티션 ID는 Int (PartitionID)
  @volatile private var dataHandler: (PartitionID, Array[Byte]) => Unit = _

  private val receivedDataQueue = new ConcurrentLinkedQueue[Array[Byte]]()

  def updateSelfPort(port: Port): Unit = {
    selfAddress = NodeAddress(selfIP, port)
  }

  def handleReceivedData(partitionID: PartitionID, data: Array[Byte]): Unit = {
    if (dataHandler != null) {
      dataHandler(partitionID, data)
    } else {
      if (data != null && data.length > 0) {
        receivedDataQueue.add(data)
      }
    }
  }

  def setCustomDataHandler(handler: (PartitionID, Array[Byte]) => Unit): Unit = {
    this.dataHandler = handler
  }

  def getReceivedData: List[Array[Byte]] = {
    receivedDataQueue.asScala.toList
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

  def shutdown(): Unit = {
    executorService.shutdown()
  }
}