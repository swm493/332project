package worker

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import io.grpc.stub.StreamObserver
import sorting.worker._
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.logging.Logger
import com.google.protobuf.ByteString
import scala.concurrent.ExecutionContext
import services.NodeID

/**
 * Worker 간의 데이터 통신(Shuffle)을 담당하는 서비스입니다.
 * [리팩토링] 이제 서버 생성 로직은 제거되고, 순수하게 gRPC 서비스 구현과
 * 클라이언트 전송(Client Side) 로직만 남았습니다.
 *
 * @param selfID         자신의 IP:Port (로깅용)
 * @param onDataReceived 외부(WorkerNode/Context)로 데이터를 전달할 콜백 함수
 */
class WorkerNetworkService(selfID: NodeID, onDataReceived: (Int, Array[Byte]) => Unit)(implicit ec: ExecutionContext)
  extends WorkerServiceGrpc.WorkerService {

  private val logger = Logger.getLogger(classOf[WorkerNetworkService].getName)

  // --- Server Side (데이터 수신 구현) ---

  /**
   * gRPC shuffle 메서드 구현 (Client Streaming)
   * 다른 Worker가 보낸 데이터를 수신합니다.
   */
  override def shuffle(responseObserver: StreamObserver[ShuffleReply]): StreamObserver[ShuffleRecord] = {
    new StreamObserver[ShuffleRecord] {
      override def onNext(req: ShuffleRecord): Unit = {
        onDataReceived(req.partitionID, req.data.toByteArray)
      }

      override def onError(t: Throwable): Unit = {
        logger.warning(s"[WorkerNetwork] Receive error from peer: ${t.getMessage}")
      }

      override def onCompleted(): Unit = {
        responseObserver.onNext(ShuffleReply(success = true))
        responseObserver.onCompleted()
      }
    }
  }

  // --- Client Side (데이터 전송 로직) ---

  private val channels = new ConcurrentHashMap[NodeID, ManagedChannel]()
  private val sendObservers = new ConcurrentHashMap[NodeID, StreamObserver[ShuffleRecord]]()

  def sendData(targetWorkerID: NodeID, partitionID: Int, data: Array[Byte]): Unit = {
    var sent = false
    var retryCount = 0
    val maxRetries = 5

    while (!sent && retryCount < maxRetries) {
      try {
        val observer = getOrCreateObserver(targetWorkerID)

        observer.synchronized {
          observer.onNext(ShuffleRecord(partitionID = partitionID, data = ByteString.copyFrom(data)))
        }
        sent = true

      } catch {
        case e: Exception =>
          // 연결 문제 발생 시 해당 옵저버 제거 후 재시도
          logger.warning(s"Send failed to $targetWorkerID (attempt ${retryCount+1}): ${e.getMessage}")
          sendObservers.remove(targetWorkerID)
          retryCount += 1
          try { Thread.sleep(500) } catch { case _: InterruptedException => }
      }
    }

    if (!sent) {
      logger.severe(s"Failed to send data to $targetWorkerID after $maxRetries attempts. Data lost?")
      // 실제 구현에서는 Dead Letter Queue에 넣거나 재시도 큐에 넣어야 할 수 있음
    }
  }

  private def getOrCreateObserver(targetWorkerID: NodeID): StreamObserver[ShuffleRecord] = {
    // 1. 채널 생성 (없으면)
    channels.computeIfAbsent(targetWorkerID, id => {
      val Array(host, p) = id.split(":")
      ManagedChannelBuilder.forAddress(host, p.toInt).usePlaintext().build()
    })

    // 2. 스트림 옵저버 생성 (없으면)
    sendObservers.computeIfAbsent(targetWorkerID, id => {
      val stub = WorkerServiceGrpc.stub(channels.get(id))
      stub.shuffle(new StreamObserver[ShuffleReply] {
        override def onNext(value: ShuffleReply): Unit = {
          // 상대방이 "잘 받았음" 응답을 보낼 때 처리 (보통은 무시하거나 로그)
        }
        override def onError(t: Throwable): Unit = {
          logger.warning(s"Error in send stream to $id: ${t.getMessage}")
          sendObservers.remove(id) // 에러난 스트림 제거
        }
        override def onCompleted(): Unit = {
          // 전송 완료
        }
      })
    })
  }

  def finishSending(): Unit = {
    val it = sendObservers.values().iterator()
    while (it.hasNext) {
      try {
        it.next().onCompleted()
      } catch {
        case e: Exception => logger.warning(s"Error completing observer: ${e.getMessage}")
      }
    }
    sendObservers.clear()
  }

  /**
   * 클라이언트용 채널을 모두 닫습니다.
   * (서버 종료는 WorkerNode가 담당하므로 여기서는 클라이언트 리소스만 정리)
   */
  def closeChannels(): Unit = {
    finishSending()

    val it = channels.values().iterator()
    while (it.hasNext) {
      try {
        it.next().shutdown().awaitTermination(1, TimeUnit.SECONDS)
      } catch {
        case e: Exception => logger.warning(s"Error shutting down channel: ${e.getMessage}")
      }
    }
    channels.clear()
    logger.info("WorkerNetworkService client channels closed.")
  }
}