package worker

import worker.WorkerContext
import services.StorageService
import worker.WorkerNetworkService
import services.WorkerState._
import sorting.sorting.{SampleRequest, NotifyRequest, ProtoKey}
import com.google.protobuf.ByteString
import scala.collection.mutable.ListBuffer

trait WorkerPhase {
  def execute(ctx: WorkerContext): WorkerState
}

class SamplingPhase extends WorkerPhase {
  override def execute(ctx: WorkerContext): WorkerState = {
    println("[Phase] Sampling Started")
    val samples = ListBuffer[Array[Byte]]()

    for (dir <- ctx.inputDirs; file <- StorageService.listFiles(dir)) {
      samples ++= StorageService.extractSamples(file)
    }

    val protoSamples = samples.map(k => ProtoKey(ByteString.copyFrom(k))).toSeq
    ctx.masterClient.submitSamples(SampleRequest(ctx.workerID, protoSamples))

    println(s"[Phase] Sampling Done. Submitted ${samples.size} samples.")
    Shuffling // 다음 상태 반환
  }
}

class ShufflePhase extends WorkerPhase {
  override def execute(ctx: WorkerContext): WorkerState = {
    println("[Phase] Shuffling Started")

    // 1. 네트워크 서비스 시작 (수정됨: 생성자 인자 1개)
    // 주의: ctx.networkService는 var여야 하며 WorkerContext에 정의되어 있어야 합니다.
    ctx.networkService = new WorkerNetworkService(ctx.workerID)

    // 메서드 이름 수정: startReceivingData -> startReceivingServer
    ctx.networkService.startReceivingServer { record =>
      // 수신된 데이터를 임시 파일에 저장하는 로직
      StorageService.saveToTempFile(ctx.outputDir, record)
    }

    // 2. 파티셔닝 및 전송 로직 (구체화됨)
    println("Start Partitioning and Sending...")

    // 예시: 라운드 로빈이나 해시 파티셔닝 로직이 필요함. 
    // 여기서는 단순히 모든 input 데이터를 순회하며 적절한 워커에게 보낸다고 가정
    for (dir <- ctx.inputDirs; file <- StorageService.listFiles(dir)) {
      val records = StorageService.readRecords(file) // 구현 필요
      for (record <- records) {
        // 파티셔너를 통해 타겟 워커 ID 결정 (WorkerContext에 partitioner가 있다고 가정)
        // val targetWorkerID = ctx.partitioner.getWorker(record) 
        // 테스트용: 자기 자신에게만 보내거나 첫 번째 워커에게 보냄
        val targetWorkerID = ctx.allWorkerIDs.head

        // 메서드 이름 수정: sendData -> sendRecord
        ctx.networkService.sendRecord(targetWorkerID, record)
      }
    }

    // 3. 완료 처리
    ctx.networkService.finishSending()

    // 주의: 셔플 단계는 '내가 보낸 것'만 끝났다고 끝나는 것이 아니라, 
    // '남들이 나에게 보내는 것'도 끝나야 합니다.
    // 보통은 Master가 Barrier를 통해 "모든 셔플 완료"를 알려줄 때까지 기다리거나
    // 모든 워커로부터 "전송 완료" 신호를 받아야 합니다.
    // 여기서는 일단 sleep으로 대기하거나 Master의 신호를 기다리는 로직이 필요합니다.

    // 임시: 충분히 기다렸다고 가정하고 종료 (실제 분산 환경에서는 Master의 신호 필요)
    Thread.sleep(5000)

    ctx.networkService.shutdown()

    ctx.masterClient.notifyShuffleComplete(NotifyRequest(ctx.workerID))
    println("[Phase] Shuffling Done.")
    Merging
  }
}

class MergePhase extends WorkerPhase {
  override def execute(ctx: WorkerContext): WorkerState = {
    println("[Phase] Merging Started")

    val receivedFiles = StorageService.getReceivedTempFiles(ctx.outputDir)
    // 병합 정렬 로직 (구현 필요)
    // StorageService.mergeFiles(receivedFiles, ctx.outputDir + "/sorted_output")

    StorageService.deleteTempFiles(receivedFiles)

    ctx.masterClient.notifyMergeComplete(NotifyRequest(ctx.workerID))
    println("[Phase] Merging Done.")
    Done
  }
}