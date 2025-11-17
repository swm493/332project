package services

import services.{Key, WorkerID}

/**
 * Worker-to-Worker 통신 (셔플 단계)을 위한 인터페이스
 */
trait NetworkService {
  /**
   * 다른 워커에게 데이터를 전송합니다.
   * (worker.scala - sendDataTo)
   * @param targetWorkerID 데이터를 받을 워커의 ID (주소)
   * @param record 전송할 레코드 (100 바이트)
   */
  def sendData(targetWorkerID: WorkerID, record: Array[Byte]): Unit

  /**
   * 다른 워커들로부터 데이터를 수신하는 서버/리스너를 시작합니다.
   * (worker.scala - startReceivingData)
   * @param onDataReceived 데이터(레코드)가 도착할 때마다 호출될 콜백 함수.
   * 이 함수는 받은 데이터를 임시 파일에 저장해야 합니다.
   */
  def startReceivingData(onDataReceived: (Array[Byte]) => Unit): Unit

  /**
   * 데이터 전송(sendData)을 모두 완료했음을 알립니다.
   * (worker.scala - finishSendingData)
   */
  def finishSending(): Unit

  /**
   * 데이터 수신(startReceivingData)이 완료될 때까지 대기합니다.
   * (worker.scala - waitForReceivingData)
   */
  def awaitReceivingCompletion(): Unit

  /**
   * 네트워크 서비스 종료
   */
  def shutdown(): Unit
}

/**
 * gRPC를 사용한 NetworkService의 (스텁) 구현체.
 * 실제로는 gRPC 클라이언트(send)와 gRPC 서버(receive)를 모두 가집니다.
 */
class GrpcNetworkService(allWorkerIDs: List[WorkerID], selfID: WorkerID) extends NetworkService {

  // TODO: gRPC 클라이언트(stub)들 초기화 (self 제외)
  private val workerClients = allWorkerIDs
    .filterNot(_ == selfID)
    .map(id => (id, s"gRPC client for $id")) // Placeholder
    .toMap

  // TODO: gRPC 수신 서버 시작
  override def startReceivingData(onDataReceived: (Array[Byte]) => Unit): Unit = {
    println(s"[Network] Starting gRPC server on $selfID to receive shuffle data...")
    // TODO: gRPC 서버 스레드 시작
    // 수신된 데이터는 onDataReceived(data) 호출
  }

  override def sendData(targetWorkerID: WorkerID, record: Array[Byte]): Unit = {
    // val client = workerClients(targetWorkerID)
    // client.send(record)
    // println(s"[Network] Sending record to $targetWorkerID (stub)") // 로그가 너무 많으므로 주석 처리
  }

  override def finishSending(): Unit = {
    println("[Network] Finished sending all data.")
    // TODO: 모든 클라이언트 스트림 닫기
  }

  override def awaitReceivingCompletion(): Unit = {
    println("[Network] Waiting for all shuffle data to be received...")
    // TODO: 수신 서버가 종료 신호를 받을 때까지 대기
    println("[Network] Receiving complete.")
  }

  override def shutdown(): Unit = {
    println("[Network] Shutting down network service.")
    // TODO: gRPC 클라이언트 및 서버 종료
  }
}