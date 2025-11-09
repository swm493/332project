class Worker(masterAddress: String, inputDirs: List[String], outputDir: String) {
  val workerID = ... // 자신의 고유 ID (e.g., IP + Port)
  var state: WorkerState = WorkerState.Unregistered
  var splitters: List[Key] = null

  def start(): Unit = {
    // gRPC 클라이언트 생성
    val master = gRPC.connect(masterAddress)

    // (재)시작 루프: 죽었다가 살아나도 [cite: 278] 여기서부터 다시 시작
    while (state != WorkerState.Done) {
      try {
        // 마스터에 등록하고 현재 내가 해야 할 작업(Phase)을 받음
        val (assignedState, receivedSplitters) = master.RegisterWorker(workerID)
        this.state = assignedState
        this.splitters = receivedSplitters

        // 마스터가 지정한 단계부터 실행
        if (state == WorkerState.Sampling) {
          executeSampling(master)
          state = WorkerState.Shuffling // 셔플 대기
        }
        else if (state == WorkerState.Shuffling) {
          if (splitters == null) {
            // (예외 처리) 마스터가 아직 스플리터 계산 중
            Thread.sleep(1000)
            continue
          }
          executeShuffleAndPartition(master)
          master.NotifyShuffleComplete(workerID)
          state = WorkerState.Merging // 병합 대기
        }
        else if (state == WorkerState.Merging) {
          executeMerge()
          master.NotifyMergeComplete(workerID)
          state = WorkerState.Done
        }
      } catch (e: Exception) {
        // 마스터와 연결 끊김 등 (실패 에뮬레이션 [cite: 283])
        println("연결 실패. 5초 후 재시도...")
        Thread.sleep(5000)
        // 루프 처음으로 돌아가 마스터에 재등록
      }
    }
    println("워커 작업 완료.")
  }

  // Phase 1: 샘플링 실행
  def executeSampling(master: MasterClient): Unit = {
    val samples = new ListBuffer[Key]()
    for (dir <- inputDirs) {
      for (file <- listFiles(dir)) {
        // 입력 파일 [cite: 327]에서 샘플 추출 (10바이트 키 )
        samples += extractSamples(file)
      }
    }
    master.SubmitSamples(workerID, samples.toList)
  }

  // Phase 3: 파티션 및 셔플 실행
  def executeShuffleAndPartition(master: MasterClient): Unit = {
    // 1. 다른 워커들에게 보낼 N개의 버퍼(gRPC 스트림 또는 파일) 생성
    val outputBuffers = createNBuffers(numWorkers)

    // 2. 다른 워커들로부터 데이터를 받을 리스너/스레드 시작
    //    (받은 데이터는 로컬 디스크의 임시 파일에 저장)
    startReceivingData(outputDir)

    // 3. 모든 입력 파일 [cite: 327]을 다시 읽음
    for (dir <- inputDirs) {
      for (file <- listFiles(dir)) {
        for (record <- readRecords(file)) { // 100바이트 레코드 [cite: 9]
          val key = record.getKey() // 10바이트 키
          val targetWorker = findTargetWorker(key, splitters)

          // 4. 키에 맞는 워커에게 데이터 전송 (Shuffle)
          sendDataTo(targetWorker, record)
        }
      }
    }
    // 5. 데이터 전송 완료. 수신도 완료될 때까지 대기
    finishSendingData()
    waitForReceivingData()
  }

  // Phase 4: 병합 실행
  def executeMerge(): Unit = {
    // 셔플 단계에서 받은 모든 임시 파일 목록을 가져옴
    val receivedFiles = getReceivedTempFiles(outputDir)

    // K-way merge [cite: 64] 수행
    val finalOutputFile = createOutputFile(outputDir, "partition.N") //
    mergeSortFiles(receivedFiles, finalOutputFile)

    // 임시 파일 삭제 [cite: 342]
    deleteTempFiles(receivedFiles)
  }
}