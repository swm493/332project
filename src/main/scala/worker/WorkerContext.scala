package worker

import sorting.master.*
import services.{Key, NodeAddress, WorkerEndpoint, IP, Port, PartitionID}
import services.RecordOrdering.ordering.compare

import java.util.concurrent.{ConcurrentLinkedQueue, Executors} // Executors 추가
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

class WorkerContext(
                     val inputDirs: List[String],
                     val outputDir: String,
                     val masterClient: MasterServiceGrpc.MasterServiceBlockingStub
                   ) {
  private val selfIP: IP = services.NetworkUtils.findLocalIpAddress()

  var selfAddress: NodeAddress = NodeAddress(selfIP, 0)

  var myEndpoint: WorkerEndpoint = _
  var allWorkerEndpoints: List[WorkerEndpoint] = _
  var splitters: List[Key] = _

  var networkService: WorkerNetworkService = _

  // CPU 코어 수(4개)만큼의 스레드 풀 생성
  private val executorService = java.util.concurrent.Executors.newFixedThreadPool(4)
  val executionContext: scala.concurrent.ExecutionContext =
    scala.concurrent.ExecutionContext.fromExecutorService(executorService)

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

  def isReadyForShuffle: Boolean = splitters != null && allWorkerEndpoints != null

  def findPartitionIndex(key: Key): Int = {
    if (splitters == null || splitters.isEmpty) return 0

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