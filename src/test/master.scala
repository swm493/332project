class Master(numWorkers: Int) {
  // 워커의 상태 관리: (워커ID, 현재 상태)
  // 상태: Unregistered, Sampling, Shuffling, Merging, Done, Failed
  var workerStatus = new Map[WorkerID, WorkerState]()
  var workerSamples = new Map[WorkerID, List[Key]]()
  var globalSplitters: List[Key] = null

  // 1. 마스터 서버 시작 및 대기
  def start(): Unit = {
    // gRPC 서버 시작
    // numWorkers 만큼의 워커가 등록할 때까지 대기
    waitForWorkers()
    // 모든 워커가 등록되면 Sampling 단계 시작
    broadcastPhase(WorkerState.Sampling)
  }

  // [RPC] 워커가 마스터에 등록 (혹은 재등록)
  // 워커가 시작되거나, 죽었다가 다시 시작할 때 [cite: 278] 호출
  def RegisterWorker(workerID: WorkerID): (WorkerState, List[Key]) = {
    if (workerStatus.get(workerID) == Some(WorkerState.Failed)) {
      // 실패했던 워커가 재시작함
      val recoveryState = getRecoveryStateFor(workerID)
      workerStatus(workerID) = recoveryState
      return (recoveryState, globalSplitters) // 현재 진행 중인 단계와 스플리터 정보 반환
    } else {
      // 최초 등록
      workerStatus(workerID) = WorkerState.Sampling // 초기 상태
      if (workerStatus.size == numWorkers) {
        // 모든 워커가 준비됨
        broadcastPhase(WorkerState.Sampling)
      }
      return (WorkerState.Sampling, null)
    }
  }

  // [RPC] 워커가 샘플 제출
  def SubmitSamples(workerID: WorkerID, samples: List[Key]): Unit = {
    workerSamples(workerID) = samples
    workerStatus(workerID) = WorkerState.Shuffling // 샘플 냈으니 셔플 대기

    // 모든 워커가 샘플을 제출했는지 확인
    if (workerSamples.size == numWorkers) {
      calculateSplitters()
      broadcastPhase(WorkerState.Shuffling)
    }
  }

  // 스플리터 계산
  def calculateSplitters(): Unit = {
    val allSamples = workerSamples.values.flatten.sort() // 모든 샘플 정렬
    // numWorkers-1 개의 스플리터 선택
    globalSplitters = ...
  }

  // [RPC] 워커가 셔플 완료 보고
  def NotifyShuffleComplete(workerID: WorkerID): Unit = {
    workerStatus(workerID) = WorkerState.Merging
    // 모든 워커가 셔플을 완료했는지 확인
    if (allWorkersInState(WorkerState.Merging)) {
      broadcastPhase(WorkerState.Merging)
    }
  }

  // [RPC] 워커가 병합(최종 정렬) 완료 보고
  def NotifyMergeComplete(workerID: WorkerID): Unit = {
    workerStatus(workerID) = WorkerState.Done
    // 모든 워커가 완료했는지 확인
    if (allWorkersInState(WorkerState.Done)) {
      println("분산 정렬 완료!")
      shutdown()
    }
  }

  // (내부) 하트비트 또는 타임아웃으로 워커 실패 감지
  def detectWorkerFailure(workerID: WorkerID): Unit = {
    workerStatus(workerID) = WorkerState.Failed
    // 이 워커와 관련된 작업은 재시작 시 재할당됨
  }
}