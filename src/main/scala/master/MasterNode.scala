package master

import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future, Promise}
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

// 자동 생성된 gRPC 코드 임포트
import dist_sort.proto._
import common.{Key, Record, WorkerState}
import com.google.protobuf.ByteString

class MasterNode(port: Int, numWorkers: Int)(implicit ec: ExecutionContext) {

  // 마스터의 gRPC 서버 구현
  private val service = new MasterServiceGrpc.MasterService {

    // 워커들의 상태 관리
    private val workerStates = new ConcurrentHashMap[String, WorkerState]().asScala
    // 워커들로부터 수집된 키 샘플
    private val samples = new java.util.Vector[Key]().asScala

    // 1. 워커 등록
    override def registerWorker(req: RegisterWorkerRequest): Future[RegisterWorkerResponse] = {
      workerStates.put(req.workerAddress, WorkerState.REGISTERED)
      println(s"Worker registered: ${req.workerAddress}. Total: ${workerStates.size}/$numWorkers")

      // 모든 워커가 등록되었는지 확인
      if (workerStates.size == numWorkers) {
        println("All workers registered. Requesting samples...")
        // (실제 구현) 모든 워커에게 샘플링 시작을 요청해야 함 (워커 클라이언트 필요)
      }
      Future.successful(RegisterWorkerResponse(accepted = true))
    }

    // 2. 키 샘플 수신
    override def sendKeySamples(requestStream: io.grpc.stub.StreamObserver[Record]): io.grpc.stub.StreamObserver[SendKeySamplesResponse] = {
      new io.grpc.stub.StreamObserver[Record] {
        override def onNext(record: Record): Unit = {
          samples.append(Key(record.key)) // Key만 저장
        }
        override def onError(t: Throwable): Unit = {
          println("Error in receiving samples: ${t.getMessage}")
        }
        override def onCompleted(): Unit = {
          println(s"Sample stream completed. Total samples: ${samples.size}")
          // (실제 구현) 모든 워커로부터 샘플 수신이 완료되면 파티션 테이블 계산
          if (allSamplesReceived()) {
            val partitionTable = calculatePartitions()
            // (실제 구현) 모든 워커에게 파티션 테이블 전송
            println(s"Calculated partition table: $partitionTable")
          }
        }
      }
    }

    // 3. 파티셔닝 완료 보고
    override def reportPartitionDone(req: PartitionDoneRequest): Future[PartitionDoneResponse] = {
      workerStates.put(req.workerAddress, WorkerState.PARTITIONING)
      println(s"Worker finished partitioning: ${req.workerAddress}")
      // (실제 구현) 모든 워커가 파티셔닝 완료 시 셔플 시작 명령
      if (allWorkersInState(WorkerState.PARTITIONING)) {
        println("All workers done partitioning. Starting shuffle phase.")
        // (실제 구현) 모든 워커에게 셔플 시작 명령
      }
      Future.successful(PartitionDoneResponse(ack = true))
    }

    // 4. 셔플 완료 보고
    override def reportShuffleDone(req: ShuffleDoneRequest): Future[ShuffleDoneResponse] = {
      workerStates.put(req.workerAddress, WorkerState.SHUFFLING)
      println(s"Worker finished shuffling: ${req.workerAddress}")
      if (allWorkersInState(WorkerState.SHUFFLING)) {
        println("All workers done shuffling. Starting merge phase.")
        // (실제 구현) 모든 워커에게 병합 시작 명령
      }
      Future.successful(ShuffleDoneResponse(ack = true))
    }

    // 5. 병합 완료 보고
    override def reportMergeDone(req: MergeDoneRequest): Future[MergeDoneResponse] = {
      workerStates.put(req.workerAddress, WorkerState.DONE)
      println(s"Worker finished merging: ${req.workerAddress}")
      if (allWorkersInState(WorkerState.DONE)) {
        println("--- DISTRIBUTED SORT COMPLETE ---")
        // (실제 구현) 서버 종료
        server.shutdown()
      }
      Future.successful(MergeDoneResponse(ack = true))
    }

    // --- Helper 함수 (플레이스홀더) ---
    private def allSamplesReceived(): Boolean = workerStates.size == numWorkers // (간소화된 로직)
    private def allWorkersInState(state: WorkerState): Boolean = workerStates.values.forall(_ == state)

    private def calculatePartitions(): Seq[Partition] = {
      // (실제 구현) 샘플 정렬 -> 파티션 키 선정 -> 파티션 테이블 생성
      // 이 예제에서는 3개의 워커가 있다고 가정하고 임의의 키로 분할합니다.
      val sortedWorkers = workerStates.keys.toSeq.sorted
      val (minKey, maxKey) = (
        Key(ByteString.copyFrom(Array.fill[Byte](10)(0))),
        Key(ByteString.copyFrom(Array.fill[Byte](10)(0xFF.toByte)))
      )

      // 이 부분은 샘플을 기반으로 정교하게 만들어야 합니다.
      // 지금은 3개의 워커 주소에 대해 임의의 범위를 할당합니다.
      if (numWorkers == 3) {
        val p1Key = Key(ByteString.copyFrom("5".padTo(10, ' ').getBytes))
        val p2Key = Key(ByteString.copyFrom("K".padTo(10, ' ').getBytes))
        Seq(
          Partition(minKey, p1Key, sortedWorkers(0)),
          Partition(p1Key, p2Key, sortedWorkers(1)),
          Partition(p2Key, maxKey, sortedWorkers(2))
        )
      } else {
        // 1개 워커면 전체 범위
        Seq(Partition(minKey, maxKey, sortedWorkers(0)))
      }
    }
  }

  private val server: Server = ServerBuilder
    .forPort(port)
    .addService(MasterServiceGrpc.bindService(service, ec))
    .build()

  def start(): Unit = {
    server.start()
    println(s"Master gRPC Server started at $port")

    // (실제 구현) MasterApp에서 IP:Port를 출력하기 위한 로직
    // val hostIp = java.net.InetAddress.getLocalHost.getHostAddress
    // println(s"Master Address: $hostIp:$port")
  }

  def awaitTermination(): Unit = {
    server.awaitTermination()
  }
}