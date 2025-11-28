package master

import io.grpc.{Server, ServerBuilder}
import scala.concurrent.ExecutionContext
import java.util.logging.Logger
import sorting.master._

/**
 * MasterNode는 서버의 수명 주기(Start/Stop)와 의존성 주입만 담당합니다.
 * 실제 로직은 State와 Service로 분리되었습니다.
 */
class MasterNode(executionContext: ExecutionContext, port: Int, val numWorkers: Int) {
  private val logger = Logger.getLogger(classOf[MasterNode].getName)
  private var server: Server = _

  // 1. 상태 관리 객체 생성 (비즈니스 로직)
  private val state = new MasterState(numWorkers)

  // 2. gRPC 서비스 객체 생성 (통신 로직) - 상태 객체를 주입받음
  private val service = new MasterNetworkService(state)(executionContext)

  def start(): Unit = {
    server = ServerBuilder.forPort(port)
      .addService(MasterServiceGrpc.bindService(service, executionContext))
      .build
      .start

    logger.info(s"Master server started on port $port")

    // JVM 종료 시 서버도 우아하게 종료
    sys.addShutdownHook { stop() }
    server.awaitTermination()
  }

  private def stop(): Unit = {
    if (server != null) server.shutdown()
  }
}