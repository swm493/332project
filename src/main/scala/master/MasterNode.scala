package master

import io.grpc.{Server, ServerBuilder}
import services.Constant.Ports

import scala.concurrent.ExecutionContext
import java.util.logging.Logger
import sorting.master.*
import services.Constant.Ports.*

class MasterNode(executionContext: ExecutionContext, val numWorkers: Int) {
  private val logger = Logger.getLogger(classOf[MasterNode].getName)

  private val MasterWorkerID = s"${services.NetworkUtils.findLocalIpAddress()}:${Ports.MasterWorkerPort}"

  private var server: Server = _
  
  private val state = new MasterState(numWorkers)
  private val networkService = new MasterNetworkService(state)(executionContext)

  def start(): Unit = {
    server = ServerBuilder.forPort(MasterWorkerPort)
      .addService(MasterServiceGrpc.bindService(networkService, executionContext))
      .build
      .start

    logger.info(s"Master server started. Listening on $MasterWorkerID")

    sys.addShutdownHook { stop() }
    server.awaitTermination()
  }

  private def stop(): Unit = {
    if (server != null) server.shutdown()
  }
}