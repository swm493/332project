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

    // --- 1. 수신부 준비 ---
    val partitionStreams = new Array[BufferedOutputStream](totalPartitions)
    val outputFiles = new ListBuffer[File]()

    println(s"Initializing partition files ($myStartIdx ~ ${myEndIdx - 1})...")
    for (i <- myStartIdx until myEndIdx) {
      val f = new File(ctx.outputDir, s"partition_received_$i")
      if (f.exists()) f.delete() // 중복 방지 초기화

      outputFiles += f
      partitionStreams(i) = new BufferedOutputStream(new FileOutputStream(f, true))
    }

    // 서버 시작 (shutdown은 여기서 하지 않음!)
    ctx.networkService = new WorkerNetworkService(
      ctx.workerWorkerID,
      { (pIdx, data) =>
        if (pIdx >= myStartIdx && pIdx < myEndIdx) {
          val stream = partitionStreams(pIdx)
          stream.synchronized {
            stream.write(data)
          }
        }
      }
    )
    ctx.networkService.startServer()

    println("Server started. Waiting for Barrier synchronization...")
    try {
      ctx.masterClient.checkShuffleReady(
        sorting.master.ShuffleReadyRequest(workerId = ctx.masterWorkerID)
      )
      println("Barrier Passed! All workers represent ready. Starting Batch Sending...")
    } catch {
      case e: Exception =>
        println(s"Barrier failed: ${e.getMessage}")
      // 실패 시 처리가 필요하지만, 일단 진행하거나 재시도 로직 필요
    }

    try {
      val tempChunkDir = new File(ctx.outputDir, "temp_chunks")
      if (!tempChunkDir.exists()) tempChunkDir.mkdirs()
      val generatedChunks = new ListBuffer[(File, File)]()

      // --- [Step A] Local Sort & Indexing ---
      println("Step A: Local Sort & Indexing...")
      var chunkId = 0
      for (dir <- ctx.inputDirs; file <- StorageService.listFiles(dir)) {
        val recordsIter = StorageService.readRecords(file)
        if (recordsIter.hasNext) {
          val blockData = recordsIter.toArray
          scala.util.Sorting.quickSort(blockData)(services.RecordOrdering.ordering)
          val segments = findPartitionSegments(blockData, ctx)

          val dataFile = new File(tempChunkDir, s"chunk_$chunkId.data")
          val indexFile = new File(tempChunkDir, s"chunk_$chunkId.index")

          saveSortedBlock(blockData, dataFile)
          saveIndex(segments, indexFile)

          generatedChunks += ((dataFile, indexFile))
          chunkId += 1
        }
      }

      // --- [Step B] Batch Sending ---
      println("Step B: Batch Sending...")
      val BATCH_SIZE = 1024 * 1024
      val networkBuffers = MutableMap[(String, Int), ByteArrayOutputStream]()

      def flushBuffer(key: (String, Int)): Unit = {
        val (targetID, pId) = key
        if (networkBuffers.contains(key)) {
          val buf = networkBuffers(key)
          if (buf.size() > 0) {
            ctx.networkService.sendData(targetID, pId, buf.toByteArray)
            buf.reset()
          }
        }
      }

      for ((dataFile, indexFile) <- generatedChunks) {
        val segments = loadIndex(indexFile)
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
                // 내 데이터도 로컬 파일에 안전하게 씀 (Merge때 사용)
                partitionStreams(seg.partitionId).write(chunkBytes)
              } else {
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

      // 클라이언트(보내는 기능)만 종료. 서버(받는 기능)는 살려둠!
      ctx.networkService.finishSending()

      println("Shuffle Phase Logic Complete. Waiting for global sync...")

      // [수정] 파일 삭제 코드 제거함 (Merge 단계나 디버깅을 위해 유지)
      // generatedChunks.foreach(...) -> 삭제 안 함

    } finally {
      // 파일 스트림은 안전하게 닫아줘야 데이터가 저장됨
      for (s <- partitionStreams) if (s != null) {
        s.flush(); s.close()
      }

    }

    ctx.masterClient.notifyShuffleComplete(NotifyRequest(ctx.masterWorkerID))
    println("[Phase] Shuffling Done.")
    Merging
  }

  private def findPartitionSegments(sortedData: Array[Array[Byte]], ctx: WorkerContext): List[PartitionSegment] = {
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
        segments += PartitionSegment(currentPIdx, startOffset, count * recordSize)
        currentPIdx = pIdx
        startOffset = i * recordSize
        count = 0
      }
      count += 1
    }
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
        segments += PartitionSegment(dis.readInt(), dis.readLong(), dis.readInt())
      }
    } catch {
      case _: EOFException =>
    } finally {
      dis.close()
    }
    segments.toList
  }
}

class MergePhase extends WorkerPhase {
  override def execute(ctx: WorkerContext): WorkerState = {
    println("[Phase] Merging Started")

    // [핵심] 이제 더 이상 받을 데이터가 없으므로 네트워크 서비스를 종료합니다.
    if (ctx.networkService != null) {
      println("Shutting down Worker Network Service...")
      ctx.networkService.shutdown()
    }

    // Merge 로직 수행 (기존 로직 유지)
    // ...

    ctx.masterClient.notifyMergeComplete(NotifyRequest(ctx.masterWorkerID))
    println("[Phase] Merging Done.")
    Done
  }
}