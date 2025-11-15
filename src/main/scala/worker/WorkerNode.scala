package worker

import io.grpc.{ManagedChannel, ManagedChannelBuilder, Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future, Promise}
import java.io.File
import java.nio.file.Path

// 자동 생성된 gRPC 코드 임포트
import dist_sort.proto._
import services.{RecordReader, RecordWriter}
import common.{Key, Record}

class WorkerNode(
                  workerPort: Int,
                  masterAddress: String,
                  inputDirs: Seq[Path],
                  outputDir: Path
                )(implicit ec: ExecutionContext) {

  // --- gRPC 클라이언트 설정 ---
  // 마스터와 통신하기 위한 클라이언트
  private val masterChannel: ManagedChannel = ManagedChannelBuilder.forTarget(masterAddress).usePlaintext().build()
  private val masterStub = MasterServiceGrpc.stub(masterChannel)

  // (실제 구현) 다른 워커들과 통신하기 위한 클라이언트 맵
  // private var workerStubs: Map[String, WorkerServiceGrpc.WorkerServiceStub] = Map.empty

  // --- gRPC 서버 설정 (마스터 및 다른 워커의 요청 수신) ---
  private val service = new WorkerServiceGrpc.WorkerService {

    // 1. 마스터로부터 파티션 테이블 수신 (정렬 시작)
    override def sendPartitionTable(req: SendPartitionTableRequest): Future[SendPartitionTableResponse] = {
      println("Received partition table from master. Starting local sort and partition...")

      // (실제 구현) 다른 워커 주소 저장 -> gRPC 클라이언트 생성
      // val workerAddresses = req.partitions.map(_.workerAddress)
      // workerStubs = ...

      // 비동기 실행
      Future {
        try {
          // (핵심 로직 1) 로컬 정렬 및 파티셔닝
          localSortAndPartition(req.partitions)

          // 마스터에게 파티셔닝 완료 보고
          masterStub.reportPartitionDone(PartitionDoneRequest(myAddress))
        } catch {
          case e: Exception => println(s"Error during partitioning: ${e.getMessage}")
        }
      }
      Future.successful(SendPartitionTableResponse(ack = true))
    }

    // 2. 다른 워커로부터 셔플 데이터 수신
    override def shuffleData(requestStream: io.grpc.stub.StreamObserver[Record]): io.grpc.stub.StreamObserver[ShuffleDataResponse] = {
      new io.grpc.stub.StreamObserver[Record] {
        // (실제 구현) 셔플된 데이터를 임시 파일에 저장해야 함
        // val tempWriter = new RecordWriter(outputDir.resolve(s"temp_shuffle_${System.nanoTime()}"))

        override def onNext(record: Record): Unit = {
          // tempWriter.write(record)
        }
        override def onError(t: Throwable): Unit = {
          println(s"Error receiving shuffle data: ${t.getMessage}")
          // tempWriter.close()
        }
        override def onCompleted(): Unit = {
          println("Shuffle stream completed.")
          // tempWriter.close()
        }
      }
    }

    // 3. 마스터로부터 병합 시작 명령 수신
    override def startMerge(req: StartMergeRequest): Future[StartMergeResponse] = {
      println("Received merge start signal from master.")
      Future {
        // (핵심 로직 3) 병합
        mergePartitions()
        // 마스터에게 병합 완료 보고
        masterStub.reportMergeDone(MergeDoneRequest(myAddress))
      }
      Future.successful(StartMergeResponse(ack = true))
    }
  }

  private val server: Server = ServerBuilder
    .forPort(workerPort)
    .addService(WorkerServiceGrpc.bindService(service, ec))
    .build()

  private val myAddress = s"${java.net.InetAddress.getLocalHost.getHostAddress}:${server.getPort}"

  // --- 핵심 로직 함수 (플레이스홀더) ---

  private def localSortAndPartition(partitions: Seq[Partition]): Unit = {
    // (핵심 로직 1)
    println(s"Starting local sort/partition for ${inputDirs.size} directories...")

    // 1. 모든 입력 파일에서 레코드 읽기
    val allFiles = inputDirs.flatMap(dir => new File(dir.toUri).listFiles().map(_.toPath))
    val records = allFiles.flatMap(file => new RecordReader(file)).toIterator

    // 2. 인메모리 정렬 (대용량의 경우 외부 정렬 필요)
    val sortedRecords = records.toSeq.sorted(RecordOrdering)
    println(s"Read and sorted ${sortedRecords.size} records locally.")

    // 3. 파티션 테이블 기준으로 레코드를 분할
    // (실제 구현) 각 파티션에 해당하는 레코드를 임시 파일로 저장
    // (실제 구현) 이 임시 파일들을 셔플 단계에서 다른 워커로 전송

    // (핵심 로직 2) 셔플 (파티셔닝 완료 후 즉시 시작)
    println("Partitioning done. Starting shuffle...")
    // (실제 구현) partitions.foreach { p => ... workerStubs(p.workerAddress).shuffleData(...) }

    // 셔플 완료 후 마스터에게 보고
    masterStub.reportShuffleDone(ShuffleDoneRequest(myAddress))
  }

  private def mergePartitions(): Unit = {
    // (핵심 로직 3)
    println("Merging received shuffle data...")
    // (실제 구현) 모든 셔플 임시 파일을 (K-way merge)하여 최종 출력 파일 (partition.N) 생성
    // 예: outputDir.resolve("partition.001")
  }

  // Record를 Key 기준으로 정렬하기 위한 Ordering
  object RecordOrdering extends Ordering[Record] {
    def compare(a: Record, b: Record): Int = a.key.compare(b.key)
  }

  // --- 워커 시작 로직 ---

  def start(): Unit = {
    server.start()
    println(s"Worker gRPC Server started at $myAddress")

    // (핵심 로직 0) 마스터에게 등록
    println(s"Registering to master at $masterAddress")
    masterStub.registerWorker(RegisterWorkerRequest(myAddress))

    // (실제 구현) 샘플링 로직
    // 1. 입력 파일에서 일부 레코드 샘플링
    // 2. masterStub.sendKeySamples(...)로 마스터에게 전송
  }

  def awaitTermination(): Unit = {
    server.awaitTermination()
  }
}