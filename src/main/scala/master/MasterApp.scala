package master

import scala.concurrent.ExecutionContext.Implicits.global
import java.net.InetAddress

object MasterApp {
  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      System.err.println("Usage: master <# of workers>")
      System.exit(1)
    }

    val numWorkers = args(0).toInt
    val port = 0 // 0으로 설정 시 사용 가능한 포트 자동 할당

    val masterNode = new MasterNode(port, numWorkers)

    masterNode.start()

    // (슬라이드 요구사항) 마스터 IP:Port 출력
    val hostIp = InetAddress.getLocalHost.getHostAddress
    val actualPort = masterNode.server.getPort // 자동 할당된 실제 포트 가져오기
    println(s"${hostIp}:${actualPort}")

    // (슬라이드 요구사항) 워커 IP 목록 출력 (이 부분은 마스터가 워커로부터 등록받아야 알 수 있음)
    // println("141.223.91.81, 141.223.91.82, 141.223.91.83") // (플레이스홀더)

    masterNode.awaitTermination()
  }
}