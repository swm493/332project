package worker

import sorting.sorting.SortingServiceGrpc
import services.{NodeIp, Key}
import worker.WorkerNetworkService

/**
 * Worker의 모든 단계(Phase)에서 공통으로 필요한 데이터 모음입니다.
 */
case class WorkerContext(
                          workerID: NodeIp,
                          inputDirs: List[String],
                          outputDir: String,
                          masterClient: SortingServiceGrpc.SortingServiceBlockingStub,
                          // 실행 중 변경되는 정보들 (Mutable)
                          var splitters: List[Key] = null,
                          var allWorkerIDs: List[NodeIp] = null,
                          var networkService: WorkerNetworkService = null
                        )