package worker

import services.{Constant, NetworkUtils, NodeAddress, PartitionID, StorageService}
import services.WorkerState.*
import sorting.master.{NotifyRequest, SampleRequest}
import sorting.common.ProtoKey
import com.google.protobuf.ByteString

import java.io.*
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters.*

trait WorkerPhase {
  def execute(ctx: WorkerContext): WorkerState
}

case class PartitionSegment(partitionId: PartitionID, offset: Long, length: Int)

class SamplingPhase extends WorkerPhase {
  override def execute(ctx: WorkerContext): WorkerState = {
    println("[Phase] Sampling Started")
    val samples = ListBuffer[Array[Byte]]()

    for (dir <- ctx.inputDirs; file <- StorageService.listFiles(dir)) {
      samples ++= StorageService.extractSamples(file)
    }

    val protoSamples = samples.map(k => ProtoKey(ByteString.copyFrom(k))).toSeq

    ctx.masterClient.submitSamples(SampleRequest(
      workerEndpoint = Some(NetworkUtils.workerEndpointToProto(ctx.myEndpoint)),
      samples = protoSamples
    ))

    println(s"[Phase] Sampling Done. Submitted ${samples.size} samples.")
    Shuffling
  }
}

class ShufflePhase extends WorkerPhase {
  override def execute(ctx: WorkerContext): WorkerState = {
    println("[Phase] Shuffling Started")

    val P = Constant.Size.partitionPerWorker
    val totalPartitions = ctx.allWorkerEndpoints.length * P

    val myWorkerIdx = ctx.allWorkerEndpoints.indexOf(ctx.myEndpoint)
    val myStartIdx = myWorkerIdx * P
    val myEndIdx = myStartIdx + P

    // 1. 수신부 준비
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

    // 서버 시작 (수신 콜백)
    ctx.setCustomDataHandler { (pIdx, data) =>
      if (pIdx >= myStartIdx && pIdx < myEndIdx) {
        val stream = partitionStreams(pIdx)
        val idxStream = partitionIndexStreams(pIdx)
        stream.synchronized {
          stream.write(data)
          idxStream.writeInt(data.length)
        }
      }
    }

    println("Ready to receive data. Waiting for Barrier synchronization...")
    try {
      ctx.masterClient.checkShuffleReady(
        sorting.master.ShuffleReadyRequest(workerEndpoint = Some(NetworkUtils.workerEndpointToProto(ctx.myEndpoint)))
      )
      println("Barrier Passed! Starting Batch Sending...")
    } catch {
      case e: Exception =>
        println(s"Barrier failed: ${e.getMessage}")
    }

    try {
      val tempChunkDir = new File(ctx.outputDir, "temp_chunks")
      if (!tempChunkDir.exists()) tempChunkDir.mkdirs()
      val generatedChunks = new ConcurrentLinkedQueue[(File, File)]()

      println("Step A: Local Sort & Indexing...")

      val allInputFiles = ctx.inputDirs.flatMap(dir => StorageService.listFiles(dir))

      var chunkId = 0

      val sortFutures = allInputFiles.zipWithIndex.map { case (file, idx) =>
        Future {
          val recordsIter = StorageService.readRecords(file)
          val currentBlock = new ListBuffer[Array[Byte]]()
          var currentSize = 0L
          var subChunkId = 0

          while (recordsIter.hasNext) {
            val record = recordsIter.next()
            currentBlock += record
            currentSize += record.length

            if (currentSize >= services.Constant.Size.block) {
              val uniqueId = s"${idx}_${subChunkId}"
              processBlock(currentBlock.toArray, uniqueId, ctx, tempChunkDir, generatedChunks)
              currentBlock.clear()
              currentSize = 0
              subChunkId += 1
            }
          }
          if (currentBlock.nonEmpty) {
            val uniqueId = s"${idx}_${subChunkId}"
            processBlock(currentBlock.toArray, uniqueId, ctx, tempChunkDir, generatedChunks)
          }
        }(ctx.executionContext) // WorkerContext의 스레드 풀 사용
      }

      Await.result(Future.sequence(sortFutures), Duration.Inf)
      println(s"Step A Complete. Generated ${generatedChunks.size()} chunks.")

      println("Step B: Batch Sending...")

      for ((dataFile, indexFile) <- generatedChunks.asScala) {
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
                val stream = partitionStreams(seg.partitionId)
                val idxStream = partitionIndexStreams(seg.partitionId)
                stream.synchronized {
                  stream.write(chunkBytes)
                  idxStream.writeInt(chunkBytes.length)
                }
              } else {
                ctx.networkService.sendData(targetEndpoint.address, seg.partitionId, chunkBytes)
              }
            }
          }
        } finally {
          raf.close()
        }
      }

      if (ctx.networkService != null) ctx.networkService.finishSending()

      println("Sending complete. Deleting temporary chunks...")

      generatedChunks.asScala.foreach { case (dataFile, indexFile) =>
        if (dataFile.exists()) dataFile.delete()
        if (indexFile.exists()) indexFile.delete()
      }
      if (tempChunkDir.exists()) tempChunkDir.delete()

      println("Notifying Master and waiting for Global Sync...")

      ctx.masterClient.notifyShuffleComplete(NotifyRequest(workerEndpoint = Some(NetworkUtils.workerEndpointToProto(ctx.myEndpoint))))

      var globalDone = false
      while (!globalDone) {
        val hb = ctx.masterClient.heartbeat(sorting.master.HeartbeatRequest(workerEndpoint = Some(NetworkUtils.workerEndpointToProto(ctx.myEndpoint))))
        if (hb.state == sorting.master.HeartbeatReply.WorkerHeartState.Merging) {
          globalDone = true
        } else {
          Thread.sleep(100)
        }
      }
      println("Global Shuffle Complete! Proceeding to cleanup.")

    } finally {
      for (s <- partitionStreams) if (s != null) { s.flush(); s.close() }
      for (s <- partitionIndexStreams) if (s != null) { s.flush(); s.close() }
      ctx.setCustomDataHandler(null)
    }

    Merging
  }

  private def processBlock(blockData: Array[Array[Byte]], id: String, ctx: WorkerContext, tempChunkDir: File, generatedChunks: java.util.Queue[(File, File)]): Unit = {
    scala.util.Sorting.quickSort(blockData)(services.RecordOrdering.ordering)

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
  override def execute(ctx: WorkerContext): WorkerState = {
    println("[Phase] Merging Started")

    if (ctx.networkService != null) ctx.networkService.closeChannels()

    val P = Constant.Size.partitionPerWorker
    val myWorkerIdx = ctx.allWorkerEndpoints.indexOf(ctx.myEndpoint)
    val myStartIdx = myWorkerIdx * P
    val myEndIdx = myStartIdx + P

    val mergeFutures = (myStartIdx until myEndIdx).map { pId =>
      Future {
        val dataFile = new File(ctx.outputDir, s"partition_received_$pId")
        val indexFile = new File(ctx.outputDir, s"partition_received_$pId.index")
        val finalFile = new File(ctx.outputDir, s"partition.$pId")

        if (dataFile.exists() && indexFile.exists()) {
          // println(s"Merging partition $pId...")
          val chunkLengths = loadChunkLengths(indexFile)
          val raf = new RandomAccessFile(dataFile, "r")
          val iterators = createChunkIterators(raf, chunkLengths)

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
          new FileOutputStream(finalFile).close()
        }
      }(ctx.executionContext) // 스레드 풀 사용
    }

    Await.result(Future.sequence(mergeFutures), Duration.Inf)

    ctx.masterClient.notifyMergeComplete(NotifyRequest(workerEndpoint = Some(NetworkUtils.workerEndpointToProto(ctx.myEndpoint))))
    println("[Phase] Merging Done.")
    Done
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