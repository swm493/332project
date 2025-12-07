package utils

import java.net.{InetAddress, NetworkInterface}
import scala.jdk.CollectionConverters._

object NetworkUtils {
  def findLocalIpAddress(): IP = {
    try {
      NetworkInterface.getNetworkInterfaces.asScala
        .filter(i => !i.isLoopback && i.isUp)
        .flatMap(_.getInetAddresses.asScala)
        .find(addr => addr.isSiteLocalAddress)
        .map(_.getHostAddress)
        .getOrElse(InetAddress.getLocalHost.getHostAddress)
    } catch {
      case _: Exception => "Unknown-IP"
    }
  }

  def workerEndpointToProto(domain: utils.WorkerEndpoint): sorting.common.WorkerEndpoint = {
    sorting.common.WorkerEndpoint(
      id = domain.id.toString,
      address = Some(sorting.common.NodeAddress(domain.address.ip, domain.address.port))
    )
  }

  def workerProtoToEndpoint(proto: sorting.common.WorkerEndpoint): utils.WorkerEndpoint = {
    val addr = proto.address.get
    utils.WorkerEndpoint(proto.id.toInt, utils.NodeAddress(addr.ip, addr.port))
  }
}