package worker

import io.grpc.{ManagedChannel, ManagedChannelBuilder, Server, ServerBuilder}
import io.grpc.stub.StreamObserver
import sorting.worker._
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.logging.Logger
import com.google.protobuf.ByteString
import scala.concurrent.ExecutionContext
import services.NodeID

/**
 * Worker 간의 데이터 통신(Shuffle)을 담당하는 서비스입니다.
 * MasterNetworkService와 동일하게 gRPC 인터페이스를 직접 구현합니다.
 * * @param selfID         자신의 IP:Port (서버 포트 결정용)
 * @param onDataReceived 외부(WorkerState 등)로 데이터를 전달할 콜백 함수
 */
class WorkerNetworkService(selfID: NodeID, onDataReceived: (Int, Array[Byte]) => Unit)(implicit ec: ExecutionContext)
  extends WorkerServiceGrpc.WorkerService {

  private val logger = Logger.getLogger(classOf[WorkerNetworkService].getName)
  private val port = selfID.split(":")(1).toInt

  // --- Server Side (데이터 수신 구현) ---

  private var server: Server = _

  /**
   * 서버를 시작합니다.
   * 내부 클래스 없이 `this`를 사용하여 서비스를 바인딩합니다.
   */
  def startServer(): Unit = {
    server = ServerBuilder.forPort(port)
      // 여기서 new WorkerServiceImpl(...) 대신 this를 사용합니다.
      .addService(WorkerServiceGrpc.bindService(this, ec))
      .build
      .start()

    logger.info(s"[WorkerNetwork] Server started on port $port")
  }

  /**
   * gRPC shuffle 메서드 구현 (Client Streaming)
   * 다른 Worker가 보낸 데이터를 수신합니다.
   */
  override def shuffle(responseObserver: StreamObserver[ShuffleReply]): StreamObserver[ShuffleRecord] = {
    new StreamObserver[ShuffleRecord] {
      override def onNext(req: ShuffleRecord): Unit = {
        // 수신된 데이터를 콜백을 통해 로직(WorkerState)으로 전달
        onDataReceived(req.partitionID, req.data.toByteArray)
      }

      override def onError(t: Throwable): Unit = {
        logger.warning(s"[WorkerNetwork] Receive error: ${t.getMessage}")
      }

      override def onCompleted(): Unit = {
        // 수신 완료 응답 전송
        responseObserver.onNext(ShuffleReply(success = true))
        responseObserver.onCompleted()
      }
    }
  }

  // --- Client Side (데이터 전송 로직) ---
  // (기존 로직 유지: 다른 Worker에게 데이터를 보낼 때 사용)

  private val channels = new ConcurrentHashMap[NodeID, ManagedChannel]()
  private val sendObservers = new ConcurrentHashMap[NodeID, StreamObserver[ShuffleRecord]]()

  // [수정] 파티션 ID를 인자로 추가
  def sendData(targetWorkerID: NodeID, partitionID: Int, data: Array[Byte]): Unit = {
    channels.computeIfAbsent(targetWorkerID, id => {
      val Array(host, p) = id.split(":")
      ManagedChannelBuilder.forAddress(host, p.toInt).usePlaintext().build()
    })

    val observer = sendObservers.computeIfAbsent(targetWorkerID, id => {
      val stub = WorkerServiceGrpc.stub(channels.get(id))
      stub.shuffle(new StreamObserver[ShuffleReply] {
        override def onNext(value: ShuffleReply): Unit = {}
        override def onError(t: Throwable): Unit = {
          logger.warning(s"Send error to $id: ${t.getMessage}")
          sendObservers.remove(id)
        }
        override def onCompleted(): Unit = {}
      })
    })

    // [수정] 파티션 ID를 포함해서 전송
    observer.synchronized {
      observer.onNext(ShuffleRecord(data = ByteString.copyFrom(data), partitionID = partitionID))
    }
  }

  def finishSending(): Unit = {
    val it = sendObservers.values().iterator()
    while (it.hasNext) it.next().onCompleted()
    sendObservers.clear()
  }

  def shutdown(): Unit = {
    finishSending()
    val it = channels.values().iterator()
    while (it.hasNext) it.next().shutdown().awaitTermination(1, TimeUnit.SECONDS)
    channels.clear()
    if (server != null) server.shutdown().awaitTermination(1, TimeUnit.SECONDS)
  }
}