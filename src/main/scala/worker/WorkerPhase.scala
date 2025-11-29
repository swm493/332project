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

    for (dir <- ctx.inputDirs; file <- StorageService.listFiles(dir)) {
      samples ++= StorageService.extractSamples(file)
    }

    val protoSamples = samples.map(k => ProtoKey(ByteString.copyFrom(k))).toSeq
    ctx.masterClient.submitSamples(SampleRequest(ctx.masterWorkerID, protoSamples))

    println(s"[Phase] Sampling Done. Submitted ${samples.size} samples.")
    Shuffling
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

    // --- 1. 수신부 준비 (파일 스트림만 준비) ---
    val partitionStreams = new Array[BufferedOutputStream](totalPartitions)
    val partitionIndexStreams = new Array[DataOutputStream](totalPartitions)
    val outputFiles = new ListBuffer[File]()

    println(s"Initializing partition files ($myStartIdx ~ ${myEndIdx - 1})...")
    for (i <- myStartIdx until myEndIdx) {
      val fData = new File(ctx.outputDir, s"partition_received_$i")
      val fIndex = new File(ctx.outputDir, s"partition_received_$i.index")
      if (fData.exists()) fData.delete()
      if (fIndex.exists()) fIndex.delete()

      outputFiles += fData
      outputFiles += fIndex
      partitionStreams(i) = new BufferedOutputStream(new FileOutputStream(fData, true))
      partitionIndexStreams(i) = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fIndex, true)))
    }

    // [수정] 서버를 직접 켜지 않고, Context에 핸들러를 등록합니다.
    // WorkerNode가 이미 켜둔 서버가 데이터를 받으면 이 로직이 실행됩니다.
    ctx.setCustomDataHandler { (pIdx, data) =>
      if (pIdx >= myStartIdx && pIdx < myEndIdx) {
        val stream = partitionStreams(pIdx)
        val idxStream = partitionIndexStreams(pIdx)
        // 여러 스레드(gRPC)에서 동시에 들어오므로 동기화 필수
        stream.synchronized {
          stream.write(data)
          idxStream.writeInt(data.length)
        }
      }
    }

    println("Ready to receive data. Waiting for Barrier synchronization...")
    try {
      ctx.masterClient.checkShuffleReady(
        sorting.master.ShuffleReadyRequest(workerId = ctx.masterWorkerID)
      )
      println("Barrier Passed! Starting Batch Sending...")
    } catch {
      case e: Exception =>
        println(s"Barrier failed: ${e.getMessage}")
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
                // 내 데이터 -> 바로 로컬 파일에 (인덱스 포함)
                val stream = partitionStreams(seg.partitionId)
                val idxStream = partitionIndexStreams(seg.partitionId)
                stream.synchronized {
                  stream.write(chunkBytes)
                  idxStream.writeInt(chunkBytes.length)
                }
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

      println("Sending complete. Deleting temporary chunks...")

      generatedChunks.foreach { case (dataFile, indexFile) =>
        if (dataFile.exists()) dataFile.delete()
        if (indexFile.exists()) indexFile.delete()
      }

      if (tempChunkDir.exists()) tempChunkDir.delete()

      println("Notifying Master and waiting for Global Sync...")

      ctx.masterClient.notifyShuffleComplete(NotifyRequest(ctx.masterWorkerID))

      var globalDone = false
      while (!globalDone) {
        val hb = ctx.masterClient.heartbeat(sorting.master.HeartbeatRequest(ctx.masterWorkerID))
        if (hb.state == sorting.master.HeartbeatReply.WorkerHeartState.Merging) {
          globalDone = true
        } else {
          Thread.sleep(100)
        }
      }
      println("Global Shuffle Complete! Proceeding to cleanup.")

    } finally {
      // 리팩토링: 서버 종료(shutdown)는 WorkerNode가 담당하므로 여기서는 하지 않음
      // ctx.networkService.shutdown() -> 삭제됨

      for (s <- partitionStreams) if (s != null) { s.flush(); s.close() }
      for (s <- partitionIndexStreams) if (s != null) { s.flush(); s.close() }

      // 핸들러 해제 (안전장치)
      ctx.setCustomDataHandler(null)
    }

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

    val P = Constant.Size.partitionPerWorker
    val myWorkerIdx = ctx.allWorkerIDs.indexOf(ctx.workerWorkerID)
    val myStartIdx = myWorkerIdx * P
    val myEndIdx = myStartIdx + P

    // 각 파티션별로 병합 수행
    for (pId <- myStartIdx until myEndIdx) {
      val dataFile = new File(ctx.outputDir, s"partition_received_$pId")
      val indexFile = new File(ctx.outputDir, s"partition_received_$pId.index")
      val finalFile = new File(ctx.outputDir, s"partition.$pId")

      if (dataFile.exists() && indexFile.exists()) {
        println(s"Merging partition $pId...")

        // 1. 인덱스 로드 (각 청크의 길이 정보)
        val chunkLengths = loadChunkLengths(indexFile)

        // 2. 데이터 파일을 쪼개서 Iterator들 생성
        // (주의: RAF는 메모리를 많이 쓰지 않지만, Iterator 개수가 많으면 파일 핸들 문제 주의)
        val raf = new RandomAccessFile(dataFile, "r")
        val iterators = createChunkIterators(raf, chunkLengths)

        // 3. K-way Merge 실행
        val bos = new BufferedOutputStream(new FileOutputStream(finalFile))
        try {
          services.MergeService.merge(
            iterators,
            record => bos.write(record)
          )
        } finally {
          bos.close()
          raf.close()
        }
      } else {
        // 데이터 없는 파티션 처리 (빈 파일 생성)
        new FileOutputStream(finalFile).close()
      }
    }

    ctx.masterClient.notifyMergeComplete(NotifyRequest(ctx.masterWorkerID))
    println("[Phase] Merging Done.")
    Done
  }

  // --- Helpers ---
  private def loadChunkLengths(file: File): List[Int] = {
    val dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))
    val lengths = new ListBuffer[Int]()
    try {
      while (dis.available() > 0) lengths += dis.readInt()
    } catch { case _: EOFException => } finally { dis.close() }
    lengths.toList
  }

  // Helper: RAF를 공유하는 커스텀 Iterator 생성
  private def createChunkIterators(raf: RandomAccessFile, lengths: List[Int]): Seq[Iterator[Array[Byte]]] = {
    var currentOffset = 0L
    val iterators = new ListBuffer[Iterator[Array[Byte]]]()
    for (len <- lengths) {
      iterators += new OnDemandChunkIterator(raf, currentOffset, len)
      currentOffset += len
    }
    iterators.toSeq
  }
}

// [Custom Iterator] 파일의 특정 구간만 읽어오는 이터레이터
class OnDemandChunkIterator(raf: RandomAccessFile, startOffset: Long, length: Int) extends Iterator[Array[Byte]] {
  private var readBytes = 0
  private val recordSize = Constant.Size.record

  override def hasNext: Boolean = readBytes < length

  override def next(): Array[Byte] = {
    if (!hasNext) throw new NoSuchElementException
    val buffer = new Array[Byte](recordSize)

    raf.synchronized {
      raf.seek(startOffset + readBytes)
      raf.readFully(buffer)
    }
    readBytes += recordSize
    buffer
  }
}