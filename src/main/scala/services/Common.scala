package services

import java.net.{InetAddress, NetworkInterface}
import scala.jdk.CollectionConverters._
/**
 * 프로젝트 전체에서 사용되는 공통 타입과 상태를 정의합니다.
 */

// [추가] 직관적인 타입 별칭 정의
type IP = String
type Port = Int
type ID = Int           // Worker ID (0-based)
type PartitionID = Int  // 데이터 파티션 ID

// 10 바이트 키
type Key = Array[Byte]

case class NodeAddress(ip: IP, port: Port) {
  override def toString: String = s"$ip:$port"
}

case class WorkerEndpoint(id: ID, address: NodeAddress)

case class MasterEndpoint(address: NodeAddress)

object Constant {
  object Size {
    def key: Int = 10
    def value: Int = 90
    def record: Int = 100
    def partitionPerWorker: Int = 10
    def block: Int = 32 * (1024 * 1024 - 1)
  }
  object Sample {
    def n: Int = 1000
  }

  object Ports {
    def MasterWorkerPort: Port = 1557
    def WorkerWorkerPort: Port = 6974
  }
}

object RecordOrdering {
  implicit val ordering: Ordering[Array[Byte]] = new Ordering[Array[Byte]] {
    override def compare(x: Array[Byte], y: Array[Byte]): Int = {
      var i = 0
      while (i < Constant.Size.key) {
        val a = x(i) & 0xFF
        val b = y(i) & 0xFF

        if (a != b) {
          return a - b
        }
        i += 1
      }
      0
    }
  }
}

object WorkerState extends Enumeration {
  type WorkerState = Value

  /**
   * Unregistered: 마스터에 등록되기 전
   * Sampling: (Phase 1) 입력 파일에서 샘플을 추출하는 단계
   * Shuffling: (Phase 2) 샘플 제출 후, 셔플 및 파티션을 대기/수행하는 단계
   * Merging: (Phase 3) 셔플된 데이터를 K-way merge 하는 단계
   * Done: 모든 작업 완료
   * Failed: 작업 중 실패 (마스터가 감지)
   */
  val Unregistered, Sampling, Shuffling, Merging, Done, Failed, Waiting = Value
}

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

  def workerEndpointToProto(domain: services.WorkerEndpoint): sorting.common.WorkerEndpoint = {
    sorting.common.WorkerEndpoint(
      id = domain.id.toString,
      address = Some(sorting.common.NodeAddress(domain.address.ip, domain.address.port))
    )
  }
}