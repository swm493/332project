package worker

import io.grpc.{ManagedChannel, ManagedChannelBuilder, Server, ServerBuilder}
import io.grpc.stub.StreamObserver
import sorting.sorting._
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.logging.Logger
import com.google.protobuf.ByteString
import scala.concurrent.ExecutionContext
import services.NodeIp

class WorkerNetworkService(selfID: NodeIp) {
  private val logger = Logger.getLogger(classOf[WorkerNetworkService].getName)

  // --- Server Side (데이터 수신) ---
  private var server: Server = _
  private val port = selfID.split(":")(1).toInt

  def startReceivingServer(onDataReceived: Array[Byte] => Unit): Unit = {
    server = ServerBuilder.forPort(port)
      .addService(WorkerServiceGrpc.bindService(new WorkerServiceImpl(onDataReceived), ExecutionContext.global))
      .build
      .start()

    logger.info(s"[WorkerNetwork] Server started on port $port")
  }

  // 서버가 종료될 때까지 대기하는 메서드 추가 (필요시 사용)
  def awaitTermination(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private class WorkerServiceImpl(callback: Array[Byte] => Unit) extends WorkerServiceGrpc.WorkerService {
    override def shuffle(responseObserver: StreamObserver[ShuffleReply]): StreamObserver[ShuffleRecord] = {
      new StreamObserver[ShuffleRecord] {
        override def onNext(req: ShuffleRecord): Unit = {
          callback(req.data.toByteArray)
        }
        override def onError(t: Throwable): Unit = {
          logger.warning(s"[WorkerNetwork] Receive error: ${t.getMessage}")
        }
        override def onCompleted(): Unit = {
          responseObserver.onNext(ShuffleReply(success = true))
          responseObserver.onCompleted()
        }
      }
    }
  }

  // --- Client Side (데이터 전송) ---
  private val channels = new ConcurrentHashMap[NodeIp, ManagedChannel]()
  private val observers = new ConcurrentHashMap[NodeIp, StreamObserver[ShuffleRecord]]()

  def sendRecord(targetWorkerID: NodeIp, record: Array[Byte]): Unit = {
    // 1. 채널 생성
    channels.computeIfAbsent(targetWorkerID, id => {
      val Array(host, p) = id.split(":")
      ManagedChannelBuilder.forAddress(host, p.toInt).usePlaintext().build()
    })

    // 2. 스트림 생성
    observers.computeIfAbsent(targetWorkerID, id => {
      val stub = WorkerServiceGrpc.stub(channels.get(id))
      stub.shuffle(new StreamObserver[ShuffleReply] {
        override def onNext(value: ShuffleReply): Unit = {}
        override def onError(t: Throwable): Unit = logger.warning(s"Send error to $id: ${t.getMessage}")
        override def onCompleted(): Unit = logger.info(s"Finished sending to $id")
      })
    })

    // 3. 데이터 전송
    val observer = observers.get(targetWorkerID)
    observer.synchronized {
      observer.onNext(ShuffleRecord(ByteString.copyFrom(record)))
    }
  }

  def finishSending(): Unit = {
    val it = observers.values().iterator()
    while (it.hasNext) {
      it.next().onCompleted()
    }
    // 채널은 바로 닫지 않고 잠시 대기하거나, shutdown()에서 처리
  }

  def shutdown(): Unit = {
    // 클라이언트 채널 정리
    val it = channels.values().iterator()
    while (it.hasNext) {
      val channel = it.next()
      channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }
    channels.clear()
    observers.clear()

    // 서버 종료
    if (server != null) {
      server.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }
  }
}