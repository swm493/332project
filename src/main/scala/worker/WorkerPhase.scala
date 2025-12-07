package worker

import com.google.protobuf.ByteString
import sorting.common.ProtoKey
import sorting.master.SampleRequest
import utils.*

import java.io.*
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

trait WorkerPhase {
  def execute(ctx: WorkerContext): Unit
}

case class PartitionSegment(partitionId: PartitionID, offset: Long, length: Int)

class SamplingPhase extends WorkerPhase {
  override def execute(ctx: WorkerContext): Unit = {
    Logging.logInfo("[Phase] Sampling Started")
    val samples = ListBuffer[Array[Byte]]()

    for (dir <- ctx.inputDirs; file <- FileUtils.listFiles(dir)) {
      samples ++= FileUtils.extractSamples(file)
    }

    val protoSamples = samples.map(k => ProtoKey(ByteString.copyFrom(k))).toSeq

    ctx.masterClient.submitSamples(SampleRequest(
      workerEndpoint = Some(NetworkUtils.workerEndpointToProto(ctx.myEndpoint)),
      samples = protoSamples
    ))

    Logging.logInfo(s"[Phase] Sampling Done. Submitted ${samples.size} samples.")
  }
}

class PartitioningPhase extends WorkerPhase {
  override def execute(ctx: WorkerContext): Unit = {
    Logging.logInfo("[Phase] Partitioning Started (Local Sort & Spill)")

    val tempChunkDirFile = new File(ctx.outputDir, "temp_chunks")
    if (!tempChunkDirFile.exists()) tempChunkDirFile.mkdirs()
    else {
      FileUtils.listFiles(tempChunkDirFile.getAbsolutePath).foreach(_.delete())
    }

    val generatedChunks = new ConcurrentLinkedQueue[(File, File)]()
    val allInputFiles = ctx.inputDirs.flatMap(dir => FileUtils.listFiles(dir))

    val partitionFutures = allInputFiles.zipWithIndex.map { case (file, idx) =>
      Future {
        val recordsIter = FileUtils.readRecords(file)
        val currentBlock = new ListBuffer[Array[Byte]]()
        var currentSize = 0L
        var subChunkId = 0

        while (recordsIter.hasNext) {
          val record = recordsIter.next()
          currentBlock += record
          currentSize += record.length

          if (currentSize >= utils.Constant.Size.block) {
            val uniqueId = s"${idx}_$subChunkId"
            processBlock(currentBlock.toArray, uniqueId, ctx, tempChunkDirFile, generatedChunks)
            currentBlock.clear()
            currentSize = 0
            subChunkId += 1
          }
        }
        if (currentBlock.nonEmpty) {
          val uniqueId = s"${idx}_$subChunkId"
          processBlock(currentBlock.toArray, uniqueId, ctx, tempChunkDirFile, generatedChunks)
        }
      }(ctx.executionContext)
    }

    Await.result(Future.sequence(partitionFutures), Duration.Inf)
    Logging.logInfo(s"[Phase] Partitioning Complete. Generated ${generatedChunks.size()} chunks.")
  }

  private def processBlock(blockData: Array[Array[Byte]], id: String, ctx: WorkerContext, tempChunkDir: File, generatedChunks: java.util.Queue[(File, File)]): Unit = {
    scala.util.Sorting.quickSort(blockData)(utils.RecordOrdering.ordering)
    val segments = findPartitionSegments(blockData, ctx)
    val dataFile = new File(tempChunkDir, s"chunk_$id.data")
    val indexFile = new File(tempChunkDir, s"chunk_$id.index")
    saveSortedBlock(blockData, dataFile)
    saveIndex(segments, indexFile)
    generatedChunks.add((dataFile, indexFile))
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
    try { data.foreach(bos.write) } finally { bos.close() }
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
    } finally { dos.close() }
  }
}

class ShufflePhase extends WorkerPhase {
  override def execute(ctx: WorkerContext): Unit = {
    ctx.clearShuffleStates()
    ctx.refreshGlobalState()
    Logging.logInfo("[Phase] Shuffling Started (Network Transfer)")

    val P = Constant.Size.partitionPerWorker
    val totalPartitions = ctx.allWorkerEndpoints.length * P

    val myWorkerIdx = ctx.allWorkerEndpoints.indexOf(ctx.myEndpoint)
    val myStartIdx = myWorkerIdx * P
    val myEndIdx = myStartIdx + P

    val partitionStreams = new Array[BufferedOutputStream](totalPartitions)
    val partitionIndexStreams = new Array[DataOutputStream](totalPartitions)
    val outputFiles = new ListBuffer[File]()

    Logging.logInfo(s"Initializing partition files ($myStartIdx ~ ${myEndIdx - 1})...")
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

    def writeToStream(pIdx: PartitionID, data: Array[Byte]): Unit = {
      if (pIdx >= myStartIdx && pIdx < myEndIdx) {
        val stream = partitionStreams(pIdx)
        val idxStream = partitionIndexStreams(pIdx)
        try{
          stream.synchronized {
            stream.write(data)
            idxStream.writeInt(data.length)
          }
        } catch {
          case _: IOException => ctx.handleReceivedData(pIdx, data)
        }
      }
    }

    ctx.setCustomDataHandler { (pIdx, data) =>
      writeToStream(pIdx, data)
    }

    var pending = ctx.pollReceivedData()
    while (pending.isDefined) {
      val (pId, data) = pending.get
      writeToStream(pId, data)
      pending = ctx.pollReceivedData()
    }

    try {
      val tempChunkDir = new File(ctx.outputDir, "temp_chunks")
      val chunkFiles = if (tempChunkDir.exists()) {
        FileUtils.listFiles(tempChunkDir.getAbsolutePath).filter(_.getName.endsWith(".index")).map { indexFile =>
          val dataFile = new File(indexFile.getAbsolutePath.replace(".index", ".data"))
          (dataFile, indexFile)
        }
      } else Seq.empty

      for ((dataFile, indexFile) <- chunkFiles) {
        val segments = loadIndex(indexFile)
        val raf = new RandomAccessFile(dataFile, "r")
        try {
          for (seg <- segments) {
            val targetWorkerIdx = seg.partitionId / P
            if (targetWorkerIdx < ctx.allWorkerEndpoints.length) {
              val targetEndpoint = ctx.allWorkerEndpoints(targetWorkerIdx)
              val chunkBytes = new Array[Byte](seg.length)
              raf.seek(seg.offset)
              raf.readFully(chunkBytes)

              if (targetEndpoint == ctx.myEndpoint) {
                writeToStream(seg.partitionId, chunkBytes)
              } else {
                ctx.networkService.sendData(targetWorkerIdx, seg.partitionId, chunkBytes)
              }
            }
          }
        } finally {
          raf.close()
        }
      }

      Logging.logInfo("Data sending complete. Broadcasting EOS signals...")

      ctx.allWorkerEndpoints.foreach { endpoint =>
        if (endpoint.id == ctx.myEndpoint.id) {
          ctx.markPeerFinished(ctx.myEndpoint.id)
        } else {
          ctx.networkService.sendEOS(endpoint.id)
        }
      }

      if (ctx.networkService != null) ctx.networkService.finishSending()

      Logging.logInfo("Waiting for all peers to finish shuffling (EOS Check)...")
      ctx.waitForShuffleCompletion()
    } finally {
      ctx.setCustomDataHandler(null)
      Thread.sleep(1000)
      for (s <- partitionStreams) if (s != null) { s.flush(); s.close() }
      for (s <- partitionIndexStreams) if (s != null) { s.flush(); s.close() }
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
    } catch { case _: EOFException => } finally { dis.close() }
    segments.toList
  }
}

class MergePhase extends WorkerPhase {
  override def execute(ctx: WorkerContext): Unit = {
    Logging.logInfo("[Phase] Merging Started")

    val streamCache = scala.collection.mutable.Map[PartitionID, (BufferedOutputStream, DataOutputStream)]()

    try {
      var pending = ctx.pollReceivedData()
      while (pending.isDefined) {
        val (pId, data) = pending.get
        val (bos, dos) = streamCache.getOrElseUpdate(pId, {
          val fData = new File(ctx.outputDir, s"partition_received_$pId")
          val fIndex = new File(ctx.outputDir, s"partition_received_$pId.index")
          val b = new BufferedOutputStream(new FileOutputStream(fData, true))
          val d = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fIndex, true)))
          (b, d)
        })
        bos.write(data)
        dos.writeInt(data.length)
        pending = ctx.pollReceivedData()
      }
    } finally {
      streamCache.values.foreach { case (b, d) =>
        try {
          b.close()
        } catch {
          case _: Exception =>
        }
        try {
          d.close()
        } catch {
          case _: Exception =>
        }
      }
    }

    val P = Constant.Size.partitionPerWorker
    val myWorkerIdx = ctx.allWorkerEndpoints.indexOf(ctx.myEndpoint)
    val myStartIdx = myWorkerIdx * P
    val myEndIdx = myStartIdx + P

    val mergeFutures = (myStartIdx until myEndIdx).map { pId =>
      Future {
        val dataFile = new File(ctx.outputDir, s"partition_received_$pId")
        val indexFile = new File(ctx.outputDir, s"partition_received_$pId.index")
        val finalFile = new File(ctx.outputDir, s"partition.$pId")

        if (finalFile.exists()) finalFile.delete()

        if (dataFile.exists() && indexFile.exists()) {
          val chunkLengths = loadChunkLengths(indexFile)
          val raf = new RandomAccessFile(dataFile, "r")
          val iterators = createChunkIterators(raf, chunkLengths)

          val bos = new BufferedOutputStream(new FileOutputStream(finalFile))
          try {
            utils.MergeUtils.merge(
              iterators,
              record => bos.write(record)
            )
          } finally {
            bos.close()
            raf.close()
          }
        } else {
          new FileOutputStream(finalFile).close()
        }
      }(ctx.executionContext)
    }

    Await.result(Future.sequence(mergeFutures), Duration.Inf)

    Logging.logInfo("[Phase] Merging Done.")
  }

  private def loadChunkLengths(file: File): List[Int] = {
    val dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))
    val lengths = new ListBuffer[Int]()
    try { while (dis.available() > 0) lengths += dis.readInt() }
    catch { case _: EOFException => } finally { dis.close() }
    lengths.toList
  }

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