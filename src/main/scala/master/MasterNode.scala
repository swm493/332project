package master

import io.grpc.{Server, ServerBuilder}
import services.{MasterEndpoint, NodeAddress, NetworkUtils}

import scala.concurrent.ExecutionContext
import java.util.logging.Logger
import sorting.master.*

class MasterNode(executionContext: ExecutionContext, val numWorkers: Int) {
  private val logger = Logger.getLogger(classOf[MasterNode].getName)

  private var masterEndpoint: MasterEndpoint = _
  private var server: Server = _

  private val state = new MasterState(numWorkers)
  private val networkService = new MasterNetworkService(state)(executionContext)

  def start(): Unit = {
    server = ServerBuilder.forPort(0)
      .addService(MasterServiceGrpc.bindService(networkService, executionContext))
      .build
      .start

    val actualIp = NetworkUtils.findLocalIpAddress()
    val actualPort = server.getPort

    masterEndpoint = MasterEndpoint(NodeAddress(actualIp, actualPort))

    logger.info(s"Master server started. Listening on ${masterEndpoint.address}")

    sys.addShutdownHook { stop() }
    server.awaitTermination()
  }

  private def stop(): Unit = {
    if (server != null) server.shutdown()
  }
}