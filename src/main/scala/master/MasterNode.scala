package master

import io.grpc.{Server, ServerBuilder}
import scala.concurrent.ExecutionContext
import java.util.logging.Logger
import sorting.sorting.SortingServiceGrpc

/**
 * MasterNode는 이제 서버의 수명 주기(Start/Stop)만 관리합니다.
 */
class MasterNode(executionContext: ExecutionContext, port: Int, val numWorkers: Int) {
  private val logger = Logger.getLogger(classOf[MasterNode].getName)
  private var server: Server = _

  // 상태와 서비스를 조립
  private val state = new MasterState(numWorkers)
  private val service = new MasterGrpcService(state)(executionContext)

  def start(): Unit = {
    server = ServerBuilder.forPort(port)
      .addService(SortingServiceGrpc.bindService(service, executionContext))
      .build
      .start

    logger.info(s"Master server started on port $port")

    sys.addShutdownHook { stop() }
    server.awaitTermination()
  }

  private def stop(): Unit = {
    if (server != null) server.shutdown()
  }
}