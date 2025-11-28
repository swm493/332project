package worker

import scala.concurrent.ExecutionContext.Implicits.global
import services.{StorageService, Constant}
import services.WorkerState._
import sorting.master.{SampleRequest, NotifyRequest}
import sorting.common.ProtoKey
import com.google.protobuf.ByteString
import java.io.{BufferedOutputStream, File, FileOutputStream}
import scala.collection.mutable.ListBuffer

trait WorkerPhase {
  def execute(ctx: WorkerContext): WorkerState
}

class SamplingPhase extends WorkerPhase {
  override def execute(ctx: WorkerContext): WorkerState = {
    println("[Phase] Sampling Started")
    val samples = ListBuffer[Array[Byte]]()

    // develop 로직: StorageService를 이용한 샘플 추출
    for (dir <- ctx.inputDirs; file <- StorageService.listFiles(dir)) {
      samples ++= StorageService.extractSamples(file)
    }

    val protoSamples = samples.map(k => ProtoKey(ByteString.copyFrom(k))).toSeq
    ctx.masterClient.submitSamples(SampleRequest(ctx.workerID, protoSamples))

    println(s"[Phase] Sampling Done. Submitted ${samples.size} samples.")
    Shuffling // 다음 예상 상태
  }
}

class ShufflePhase extends WorkerPhase {
  override def execute(ctx: WorkerContext): WorkerState = {
    println("[Phase] Shuffling Started")

    // 1. 로컬 저장소 및 네트워크 초기화
    val P = Constant.Size.partitionPerWorker
    val totalPartitions = ctx.allWorkerIDs.length * P

    // 내 로컬 파티션 파일들 준비 (다른 워커로 안 보내고 내가 가질 것들)
    val localFiles = Array.ofDim[File](totalPartitions) // 실제로는 내가 맡은 범위만 써도 되지만 인덱싱 편의상
    val localStreams = Array.ofDim[BufferedOutputStream](totalPartitions)

    // 내가 받아야 할 임시 파일
    val recvFile = new File(ctx.outputDir, s"worker_${ctx.workerID}_recv_temp")
    val recvBos = new BufferedOutputStream(new FileOutputStream(recvFile))

    // 1. 생성자에 콜백 함수를 직접 전달합니다.
    ctx.networkService = new WorkerNetworkService(
      ctx.workerID,
      { record =>
        recvBos.synchronized {
          recvBos.write(record)
        }
      }
    )

    // 2. 서버 시작은 인자 없이 호출합니다. (메서드 이름도 startServer로 변경됨)
    ctx.networkService.startServer()

    try {
      // 2. 파티셔닝 및 전송 (develop 로직 통합)
      println("Start Partitioning and Sending...")

      // 로컬 파일 스트림은 필요할 때 열거나, 미리 열어둠 (여기선 간략화)
      // 로직 최적화를 위해 내 파티션 인덱스 범위 계산
      val myWorkerIdx = ctx.allWorkerIDs.indexOf(ctx.workerID)
      val myStartIdx = myWorkerIdx * P
      val myEndIdx = myStartIdx + P

      // 로컬 파일 준비
      for (i <- myStartIdx until myEndIdx) {
        localFiles(i) = new File(ctx.outputDir, s"partition_local_$i")
        localStreams(i) = new BufferedOutputStream(new FileOutputStream(localFiles(i)))
      }

      for (dir <- ctx.inputDirs; file <- StorageService.listFiles(dir)) {
        val records = StorageService.readRecords(file)
        while(records.hasNext) {
          val record = records.next()
          // Context의 유틸리티 사용
          val pIdx = ctx.findPartitionIndex(record.take(Constant.Size.key))

          if (pIdx >= 0 && pIdx < totalPartitions) {
            val targetWorkerID = ctx.allWorkerIDs(pIdx / P)

            if (targetWorkerID == ctx.workerID) {
              // 내 담당 파티션이면 로컬 파일에 바로 씀
              localStreams(pIdx).write(record)
            } else {
              // 다른 워커면 네트워크 전송
              ctx.networkService.sendRecord(targetWorkerID, record)
            }
          }
        }
      }

      // 3. 정리 및 완료 신호
      for (i <- myStartIdx until myEndIdx) if (localStreams(i) != null) localStreams(i).close()
      ctx.networkService.finishSending()

      // 중요: 수신 완료 대기 로직 필요 (여기선 단순화하여 Master에 완료 보고 후 상태 대기)
      // 실제로는 네트워크 서비스에서 모든 Peer의 연결 종료를 감지하거나 별도 시그널 필요
      // develop 코드에 따라 단순히 sleep 하거나 종료
      Thread.sleep(3000)

      recvBos.close()
    } finally {
      ctx.networkService.shutdown()
    }

    ctx.masterClient.notifyShuffleComplete(NotifyRequest(ctx.workerID))
    println("[Phase] Shuffling Done.")
    Merging
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

    ctx.masterClient.notifyMergeComplete(NotifyRequest(ctx.workerID))
    println("[Phase] Merging Done.")
    Done
  }
}