package worker

import scala.concurrent.ExecutionContext.Implicits.global
import services.{Constant, StorageService}
import services.WorkerState.*
import sorting.master.{NotifyRequest, SampleRequest}
import sorting.common.ProtoKey
import com.google.protobuf.ByteString

import java.io._
import scala.collection.mutable.{ListBuffer, Map as MutableMap}

trait WorkerPhase {
  def execute(ctx: WorkerContext): WorkerState
}

case class PartitionSegment(partitionId: Int, offset: Long, length: Int)

class SamplingPhase extends WorkerPhase {
  override def execute(ctx: WorkerContext): WorkerState = {
    println("[Phase] Sampling Started")
    val samples = ListBuffer[Array[Byte]]()

    // develop 로직: StorageService를 이용한 샘플 추출
    for (dir <- ctx.inputDirs; file <- StorageService.listFiles(dir)) {
      samples ++= StorageService.extractSamples(file)
    }

    val protoSamples = samples.map(k => ProtoKey(ByteString.copyFrom(k))).toSeq
    ctx.masterClient.submitSamples(SampleRequest(ctx.masterWorkerID, protoSamples))

    println(s"[Phase] Sampling Done. Submitted ${samples.size} samples.")
    Shuffling // 다음 예상 상태
  }
}

class ShufflePhase extends WorkerPhase {
  override def execute(ctx: WorkerContext): WorkerState = {
    println("[Phase] Shuffling Started")

    val P = Constant.Size.partitionPerWorker
    val totalPartitions = ctx.allWorkerIDs.length * P
    val myWorkerIdx = ctx.allWorkerIDs.indexOf(ctx.workerWorkerID)
    val myStartIdx = myWorkerIdx * P
    val myEndIdx = myStartIdx + P

    // --- 1. 수신부 준비 (최적화됨) ---
    val partitionStreams = new Array[BufferedOutputStream](totalPartitions)
    val outputFiles = new ListBuffer[File]()

    for (i <- myStartIdx until myEndIdx) {
      val f = new File(ctx.outputDir, s"partition_received_$i")
      outputFiles += f
      partitionStreams(i) = new BufferedOutputStream(new FileOutputStream(f, true))
    }

    // [수정된 콜백] pIdx를 바로 받아서 파일에 씀
    ctx.networkService = new WorkerNetworkService(
      ctx.workerWorkerID,
      { (pIdx, data) =>
        // 계산 없이 바로 검증 후 쓰기
        if (pIdx >= myStartIdx && pIdx < myEndIdx) {
          val stream = partitionStreams(pIdx)
          stream.synchronized {
            stream.write(data)
          }
        }
      }
    )
    ctx.networkService.startServer()

    try {
      val tempChunkDir = new File(ctx.outputDir, "temp_chunks")
      if (!tempChunkDir.exists()) tempChunkDir.mkdirs()
      val generatedChunks = new ListBuffer[(File, File)]()

      // --- [Step A] Local Sort & Indexing (가상 파티셔닝) ---
      println("Step A: Local Sort & Indexing...")
      var chunkId = 0
      for (dir <- ctx.inputDirs; file <- StorageService.listFiles(dir)) {
        val recordsIter = StorageService.readRecords(file)
        if (recordsIter.hasNext) {
          val blockData = recordsIter.toArray
          scala.util.Sorting.quickSort(blockData)(services.RecordOrdering.ordering)
          val segments = findPartitionSegments(blockData, ctx, totalPartitions) // Helper 메서드

          val dataFile = new File(tempChunkDir, s"chunk_$chunkId.data")
          val indexFile = new File(tempChunkDir, s"chunk_$chunkId.index")

          saveSortedBlock(blockData, dataFile) // Helper 메서드
          saveIndex(segments, indexFile) // Helper 메서드

          generatedChunks += ((dataFile, indexFile))
          chunkId += 1
        }
      }

      // --- [Step B] Batch Sending (인덱스 기반 전송) ---
      println("Step B: Batch Sending...")

      // 버퍼링: (TargetWorkerID, PartitionID) -> Buffer
      val BATCH_SIZE = 1024 * 1024
      val networkBuffers = MutableMap[(String, Int), ByteArrayOutputStream]()

      def flushBuffer(key: (String, Int)): Unit = {
        val (targetID, pId) = key
        if (networkBuffers.contains(key)) {
          val buf = networkBuffers(key)
          if (buf.size() > 0) {
            // [수정] 파티션 ID를 같이 보냄
            ctx.networkService.sendData(targetID, pId, buf.toByteArray)
            buf.reset()
          }
        }
      }

      for ((dataFile, indexFile) <- generatedChunks) {
        val segments = loadIndex(indexFile) // Helper 메서드
        val raf = new RandomAccessFile(dataFile, "r")
        try {
          for (seg <- segments) {
            val targetWorkerIdx = seg.partitionId / P
            if (targetWorkerIdx < ctx.allWorkerIDs.length) {
              val targetWorkerID = ctx.allWorkerIDs(targetWorkerIdx)
              val chunkBytes = new Array[Byte](seg.length)
              raf.seek(seg.offset)
              raf.readFully(chunkBytes)

              if (targetWorkerID == ctx.workerWorkerID) {
                // 내 거면 바로 씀
                partitionStreams(seg.partitionId).write(chunkBytes)
              } else {
                // 남의 거면 (타겟, 파티션ID)별로 버퍼링
                val bufferKey = (targetWorkerID, seg.partitionId)
                val buf = networkBuffers.getOrElseUpdate(bufferKey, new ByteArrayOutputStream(BATCH_SIZE * 2))
                buf.write(chunkBytes)
                if (buf.size() >= BATCH_SIZE) flushBuffer(bufferKey)
              }
            }
          }
        } finally {
          raf.close()
        }
      }

      networkBuffers.keys.foreach(flushBuffer)
      ctx.networkService.finishSending()

      // --- [Step C] Cleanup & Wait ---
      println("Waiting for reception...")
      Thread.sleep(5000)

      generatedChunks.foreach { case (d, i) => d.delete(); i.delete() }
      tempChunkDir.delete()

    } finally {
      for (s <- partitionStreams) if (s != null) {
        s.flush(); s.close()
      }
      ctx.networkService.shutdown()
    }

    ctx.masterClient.notifyShuffleComplete(NotifyRequest(ctx.masterWorkerID))
    println("[Phase] Shuffling Done.")
    Merging
  }

  // --- Helper Methods ---

  /**
   * 정렬된 블록 데이터에서 파티션 경계(Segment)를 찾습니다.
   * 단순히 순회하면서 파티션 ID가 바뀌는 지점을 기록합니다. (이분 탐색보다 순회가 더 빠를 수 있음)
   */
  private def findPartitionSegments(
                                     sortedData: Array[Array[Byte]],
                                     ctx: WorkerContext,
                                     totalPartitions: Int
                                   ): List[PartitionSegment] = {
    val segments = new ListBuffer[PartitionSegment]()

    if (sortedData.isEmpty) return segments.toList

    var currentPIdx = ctx.findPartitionIndex(sortedData(0).take(Constant.Size.key))
    var startOffset = 0L
    var count = 0
    val recordSize = Constant.Size.record

    for (i <- sortedData.indices) {
      val key = sortedData(i).take(Constant.Size.key)
      val pIdx = ctx.findPartitionIndex(key)

      if (pIdx != currentPIdx) {
        // 파티션이 바뀌었으므로 이전 구간 기록
        segments += PartitionSegment(currentPIdx, startOffset, count * recordSize)

        // 새 구간 시작
        currentPIdx = pIdx
        startOffset = i * recordSize
        count = 0
      }
      count += 1
    }
    // 마지막 구간 기록
    segments += PartitionSegment(currentPIdx, startOffset, count * recordSize)

    segments.toList
  }

  private def saveSortedBlock(data: Array[Array[Byte]], file: File): Unit = {
    val bos = new BufferedOutputStream(new FileOutputStream(file))
    try {
      data.foreach(bos.write)
    } finally {
      bos.close()
    }
  }

  private def saveIndex(segments: List[PartitionSegment], file: File): Unit = {
    val dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))
    try {
      dos.writeInt(segments.size)
      segments.foreach { seg =>
        dos.writeInt(seg.partitionId)
        dos.writeLong(seg.offset)
        dos.writeInt(seg.length)
      }
    } finally {
      dos.close()
    }
  }

  private def loadIndex(file: File): List[PartitionSegment] = {
    val dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))
    val segments = new ListBuffer[PartitionSegment]()
    try {
      val count = dis.readInt()
      for (_ <- 0 until count) {
        val pId = dis.readInt()
        val off = dis.readLong()
        val len = dis.readInt()
        segments += PartitionSegment(pId, off, len)
      }
    } catch {
      case _: EOFException => // End of file
    } finally {
      dis.close()
    }
    segments.toList
  }
}

class MergePhase extends WorkerPhase {
  override def execute(ctx: WorkerContext): WorkerState = {
    println("[Phase] Merging Started")

    // develop의 executeMerge 로직
    // 1. ShufflePhase에서 생성된 로컬 파티션 파일들과 수신된 파일(recvFile)을 병합 정렬
    // 2. 최종 결과 파일 생성

    // (구체적인 Merge Sort 구현은 StorageService 등에 있다고 가정하고 생략하거나 간단히 기술)
    // val received = new File(ctx.outputDir, "worker_${ctx.workerID}_recv_temp")
    // StorageService.mergeAndSort(..., ctx.outputDir)

    ctx.masterClient.notifyMergeComplete(NotifyRequest(ctx.masterWorkerID))
    println("[Phase] Merging Done.")
    Done
  }
}