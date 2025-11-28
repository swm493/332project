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
    var sent = false
    var retryCount = 0
    val maxRetries = 100 // 충분히 많이 시도 (예: 100번)

    while (!sent && retryCount < maxRetries) {
      try {
        // 1. 채널/스트림 가져오기 (없으면 생성 시도)
        val observer = getOrCreateObserver(targetWorkerID)

        // 2. 전송 시도
        observer.synchronized {
          observer.onNext(ShuffleRecord(partitionID = partitionID, data = ByteString.copyFrom(data)))
        }
        sent = true // 성공!

      } catch {
        case e: Exception =>
          // 3. 실패 시 (상대방 서버 안 켜짐 등)
          // logger.warning(s"Retrying send to $targetWorkerID... (${e.getMessage})")

          // 스트림에 문제가 생겼을 수 있으므로 제거 후 재생성 유도
          sendObservers.remove(targetWorkerID)

          retryCount += 1
          Thread.sleep(1000) // 1초 대기 후 재시도
      }
    }

    if (!sent) {
      logger.severe(s"Failed to send data to $targetWorkerID after $maxRetries attempts.")
      // 여기서 throw를 하거나 에러 처리를 해야 함
    }
  }

  // 기존 로직을 분리한 헬퍼 함수
  private def getOrCreateObserver(targetWorkerID: NodeID): StreamObserver[ShuffleRecord] = {
    channels.computeIfAbsent(targetWorkerID, id => {
      val Array(host, p) = id.split(":")
      ManagedChannelBuilder.forAddress(host, p.toInt).usePlaintext().build()
    })

    sendObservers.computeIfAbsent(targetWorkerID, id => {
      val stub = WorkerServiceGrpc.stub(channels.get(id))
      stub.shuffle(new StreamObserver[ShuffleReply] {
        override def onNext(value: ShuffleReply): Unit = {}

        override def onError(t: Throwable): Unit = {
          // 비동기 에러 발생 시 맵에서 제거하여 다음 번에 재연결 유도
          sendObservers.remove(id)
        }

        override def onCompleted(): Unit = {}
      })
    })
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