package master

import io.grpc.{Server, ServerBuilder}
import utils.{Logging, MasterEndpoint, NetworkUtils, NodeAddress}

import scala.concurrent.ExecutionContext
import sorting.master.*

class MasterNode(executionContext: ExecutionContext, val numWorkers: Int) {
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

    Logging.logEssential(s"Master server started. Listening on ${masterEndpoint.address}")

    sys.addShutdownHook { stop() }
    server.awaitTermination()
  }

  private def stop(): Unit = {
    if (server != null) server.shutdown()
  }
}