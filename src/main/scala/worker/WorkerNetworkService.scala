package worker

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import io.grpc.stub.StreamObserver
import sorting.worker.*

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext
import utils.{Logging, NodeAddress, PartitionID}

class WorkerNetworkService(onDataReceived: (PartitionID, Array[Byte]) => Unit)(implicit ec: ExecutionContext)
  extends WorkerServiceGrpc.WorkerService {

  // --- Server Side ---

  override def shuffle(responseObserver: StreamObserver[ShuffleReply]): StreamObserver[ShuffleRecord] = {
    new StreamObserver[ShuffleRecord] {
      override def onNext(req: ShuffleRecord): Unit = {
        onDataReceived(req.partitionID, req.data.toByteArray)
      }

      override def onError(t: Throwable): Unit = {
        Logging.logWarning(s"[WorkerNetwork] Receive error from peer: ${t.getMessage}")
      }

      override def onCompleted(): Unit = {
        responseObserver.onNext(ShuffleReply(success = true))
        responseObserver.onCompleted()
      }
    }
  }

  // --- Client Side ---

  private val channels = new ConcurrentHashMap[NodeAddress, ManagedChannel]()
  private val sendObservers = new ConcurrentHashMap[NodeAddress, StreamObserver[ShuffleRecord]]()

  def sendData(targetAddress: NodeAddress, partitionID: PartitionID, data: Array[Byte]): Unit = {
    try {
      val observer = getOrCreateObserver(targetAddress)
      observer.synchronized {
        observer.onNext(ShuffleRecord(
          data = ByteString.copyFrom(data),
          partitionID = partitionID
        ))
      }
    } catch {
      case e: Exception =>
        sendObservers.remove(targetAddress)
        channels.remove(targetAddress)
        throw new RuntimeException(s"Send failed to $targetAddress", e)
    }
  }

  private def getOrCreateObserver(target: NodeAddress): StreamObserver[ShuffleRecord] = {
    channels.computeIfAbsent(target, addr => {
      ManagedChannelBuilder.forAddress(addr.ip, addr.port).usePlaintext().build()
    })

    sendObservers.computeIfAbsent(target, addr => {
      val stub = WorkerServiceGrpc.stub(channels.get(addr))
      stub.shuffle(new StreamObserver[ShuffleReply] {
        override def onNext(value: ShuffleReply): Unit = {}
        override def onError(t: Throwable): Unit = {
          Logging.logWarning(s"Stream error to $addr: ${t.getMessage}")
          sendObservers.remove(addr)
        }
        override def onCompleted(): Unit = {}
      })
    })
  }

  def finishSending(): Unit = {
    val it = sendObservers.values().iterator()
    while (it.hasNext) {
      try { it.next().onCompleted() }
      catch { case e: Exception => Logging.logWarning(s"Error completing observer: ${e.getMessage}") }
    }
    sendObservers.clear()
  }

  def closeChannels(): Unit = {
    finishSending()
    val it = channels.values().iterator()
    while (it.hasNext) {
      try { it.next().shutdown().awaitTermination(1, TimeUnit.SECONDS) }
      catch { case e: Exception => Logging.logWarning(s"Error shutting down channel: ${e.getMessage}") }
    }
    channels.clear()
    Logging.logInfo("WorkerNetworkService client channels closed.")
  }
}