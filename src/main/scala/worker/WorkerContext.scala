package worker

import sorting.worker._
import sorting.master._
import services.{Key, NodeIp, Constant}
import services.RecordOrdering.ordering.compare

/**
 * Worker의 모든 단계(Phase)에서 공통으로 필요한 데이터 및 유틸리티입니다.
 */
class WorkerContext(
                     val workerID: NodeIp,
                     val inputDirs: List[String],
                     val outputDir: String,
                     val masterClient: MasterServiceGrpc.MasterServiceBlockingStub
                   ) {
  // 실행 중 변경되는 정보들 (Mutable)
  var splitters: List[Key] = _
  var allWorkerIDs: List[NodeIp] = _

  // WorkerNetworkService는 ShufflePhase에서 생성 및 관리
  var networkService: WorkerNetworkService = _

  def isReadyForShuffle: Boolean = splitters != null && allWorkerIDs != null

  /**
   * [Logic from develop] 파티션 인덱스 계산 로직
   * Splitter를 기반으로 해당 키가 어느 파티션(Worker)에 속하는지 계산합니다.
   */
  def findPartitionIndex(key: Key): Int = {
    if (splitters == null || splitters.isEmpty) return 0

    // Binary Search for optimization
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