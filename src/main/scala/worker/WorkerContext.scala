package worker

import sorting.worker._
import sorting.master._
import services.{Key, NodeID, Constant}
import services.RecordOrdering.ordering.compare
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._

/**
 * Worker의 모든 단계(Phase)에서 공통으로 필요한 데이터 및 유틸리티입니다.
 */
class WorkerContext(
                     val masterWorkerID: NodeID,
                     val workerWorkerID: NodeID,
                     val inputDirs: List[String],
                     val outputDir: String,
                     val masterClient: MasterServiceGrpc.MasterServiceBlockingStub
                   ) {
  var splitters: List[Key] = _
  var allWorkerIDs: List[NodeID] = _

  var networkService: WorkerNetworkService = _

  @volatile private var dataHandler: (Int, Array[Byte]) => Unit = _

  private val receivedDataQueue = new ConcurrentLinkedQueue[Array[Byte]]()

  /**
   * [Data Handling] WorkerNetworkService의 콜백에 의해 호출됩니다.
   * 핸들러가 설정되어 있으면 핸들러에게 위임하고, 없으면 큐에 저장합니다.
   */
  def handleReceivedData(partitionID: Int, data: Array[Byte]): Unit = {
    if (dataHandler != null) {
      dataHandler(partitionID, data)
    } else {
      if (data != null && data.length > 0) {
        receivedDataQueue.add(data)
      }
    }
  }

  /**
   * 데이터를 직접 처리할 핸들러를 설정합니다. (예: ShufflePhase에서 파일 쓰기용)
   */
  def setCustomDataHandler(handler: (Int, Array[Byte]) => Unit): Unit = {
    this.dataHandler = handler
  }

  def getReceivedData: List[Array[Byte]] = {
    receivedDataQueue.asScala.toList
  }

  def isReadyForShuffle: Boolean = splitters != null && allWorkerIDs != null

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
}