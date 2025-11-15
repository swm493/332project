package services

/**
 * "Network Service"
 * (Master, Worker 모두에 존재)
 */
trait NetworkService {
  // "Use[...]", "Gesp.Chamle[Record]" 등 불분명한 내용
  def startShuffle(satnk: Any): Unit = ??? // "Start Shuffle[Satnk]"

  def start(): Unit
  def shutdown(): Unit
}

/**
 * gRPC를 사용하는 구체적인 구현 (플레이스홀더)
 */
class GrpcNetworkService extends NetworkService {
  // gRPC 서버/클라이언트 로직을 위한 플레이스홀더
  private var server: Any = null // 실제로는 io.grpc.Server 여야 함

  def start(): Unit = {
    // server = ServerBuilder.forPort(...)
    //   .addService(...)
    //   .build()
    //   .start()
    println("gRPC Network Service started (placeholder)...")
  }

  def shutdown(): Unit = {
    // server.shutdown()
    println("gRPC Network Service shutting down (placeholder)...")
  }

  def startShuffle(satnk: Any): Unit = {
    println(s"Starting shuffle with: $satnk (placeholder)")
  }
}