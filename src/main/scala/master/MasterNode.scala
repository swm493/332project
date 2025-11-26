package master

import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}
import java.util.logging.Logger
import scala.collection.mutable.{ListBuffer, Map as MutableMap}
import services.WorkerState.*
import services.{Key, WorkerID}
import services.Constant
import services.RecordOrdering.ordering

import scala.collection.mutable
import sorting.sorting._
import com.google.protobuf.ByteString

class MasterNode(executionContext: ExecutionContext, port: Int, val numWorkers: Int) {
  // ... existing code (logger, server, variables) ...
  private val logger = Logger.getLogger(classOf[MasterNode].getName)
  private var server: Server = null
  private val workerStatus = mutable.Map[WorkerID, WorkerState]()
  private val workerSamples = mutable.Map[WorkerID, List[Key]]()
  @volatile private var globalSplitters: List[Key] = null
  @volatile private var allWorkerIDs = List[WorkerID]()

  private object SortingServiceImpl extends SortingServiceGrpc.SortingService {

    // ... existing code (registerWorker) ...
    override def registerWorker(req: RegisterRequest): Future[RegisterReply] = {
      val workerID = req.workerId
      // ... existing logic to determine state ...
      val (assignedState, splittersToSend, workersToSend) = synchronized {
        if (!workerStatus.contains(workerID)) {
          allWorkerIDs = workerID :: allWorkerIDs
        }
        val currentState = if (globalSplitters != null) {
          val savedState = workerStatus.getOrElse(workerID, Shuffling)
          if (savedState.id >= Merging.id) savedState else Shuffling
        } else {
          Sampling
        }
        workerStatus(workerID) = currentState

        // Register 시에는 편의상 데이터를 줘도 됨 (옵션)
        val splitters = if (currentState.id >= Shuffling.id) globalSplitters else null
        (currentState, splitters, allWorkerIDs)
      }

      val protoSplitters = if (splittersToSend != null) {
        splittersToSend.map(key => ProtoKey(key = ByteString.copyFrom(key)))
      } else Seq.empty

      Future.successful(RegisterReply(assignedState.id, protoSplitters, workersToSend))
    }

    // [수정] Heartbeat는 이제 상태만 반환 (가볍게)
    override def heartbeat(req: HeartbeatRequest): Future[HeartbeatReply] = {
      val workerID = req.workerId
      val currentState = synchronized {
        val s = workerStatus.getOrElse(workerID, Failed)
        if (s == Shuffling && globalSplitters == null) {
          Waiting
        } else {
          s
        }
      }

      Future.successful(
        HeartbeatReply(
          state = HeartbeatReply.WorkerState.fromName(currentState.toString).getOrElse(HeartbeatReply.WorkerState.Unregistered)
        )
      )
    }

    // [신규] 무거운 데이터를 요청할 때 호출되는 함수
    override def getGlobalState(req: GetGlobalStateRequest): Future[GetGlobalStateReply] = {
      val (splittersToSend, workersToSend) = synchronized {
        (globalSplitters, allWorkerIDs)
      }

      val protoSplitters = if (splittersToSend != null) {
        splittersToSend.map(key => ProtoKey(key = ByteString.copyFrom(key)))
      } else Seq.empty

      val protoWorkers = if (workersToSend != null) workersToSend else Seq.empty

      Future.successful(
        GetGlobalStateReply(
          splitters = protoSplitters,
          allWorkerIds = protoWorkers
        )
      )
    }

    // ... existing code (submitSamples, notifyShuffleComplete, etc.) ...
    override def submitSamples(req: SampleRequest): Future[SampleReply] = {
      val workerID = req.workerId
      val samples = req.samples.map(_.key.toByteArray).toList
      synchronized {
        workerSamples(workerID) = samples
        workerStatus(workerID) = Shuffling
        if (workerSamples.size == numWorkers) {
          calculateSplitters()
        }
      }
      Future.successful(SampleReply(ack = true))
    }

    override def notifyShuffleComplete(req: NotifyRequest): Future[NotifyReply] = {
      synchronized { workerStatus(req.workerId) = Merging }
      Future.successful(NotifyReply(ack = true))
    }

    override def notifyMergeComplete(req: NotifyRequest): Future[NotifyReply] = {
      synchronized { workerStatus(req.workerId) = Done }
      Future.successful(NotifyReply(ack = true))
    }
  }

  // ... existing code (calculateSplitters, start, stop) ...
  private def calculateSplitters(): Unit = {
    val totalPartitions = numWorkers * Constant.Size.partitionPerWorker
    val numSplitters = totalPartitions - 1
    val allSamples = workerSamples.values.flatten.toArray
    java.util.Arrays.sort(allSamples, ordering)
    val splitters = ListBuffer[Key]()
    val step = allSamples.length / totalPartitions
    for (i <- 1 to numSplitters) {
      val idx = Math.min(i * step, allSamples.length - 1)
      splitters += allSamples(idx)
    }
    globalSplitters = splitters.toList
  }

  def start(): Unit = {
    server = ServerBuilder.forPort(port).addService(SortingServiceGrpc.bindService(SortingServiceImpl, executionContext)).build.start
    server.awaitTermination()
  }
  def stop(): Unit = if (server != null) server.shutdown()
}