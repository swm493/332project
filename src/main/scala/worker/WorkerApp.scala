package worker

import java.io.File
import scala.collection.mutable.ListBuffer

/**
 * WorkerApp: 워커를 실행하는 메인 프로그램
 * (project.sorting.2025.pptx)
 * "worker <master IP:port> -I <input directory> ... -O <output directory>"
 */
object WorkerApp {

  case class Config(
                     masterAddress: String = "",
                     inputDirs: List[String] = List.empty,
                     outputDir: String = ""
                   )

  def main(args: Array[String]): Unit = {

    val config = parseArgs(args)
    if (config.isEmpty) {
      System.err.println("Usage: worker <master IP:port> -I <input dir1> [<input dir2> ...] -O <output dir>")
      System.exit(1)
    }

    val conf = config.get

    // (project.sorting.2025.pptx - "do not assume a specific port")
    // 워커도 셔플 데이터를 받기 위한 포트가 필요합니다.
    // 여기서는 간단히 자신의 IP와 임의의 포트(예: 8081)를 사용합니다.
    val selfIP = java.net.InetAddress.getLocalHost.getHostAddress
    val selfPort = 8081 // TODO: 하드코딩된 포트 대신 사용 가능한 포트 찾기
    val workerID = s"$selfIP:$selfPort"

    val workerNode = new WorkerNode(
      workerID,
      conf.masterAddress,
      conf.inputDirs,
      conf.outputDir
    )

    workerNode.start()
  }

  // (project.sorting.2025.pptx)의 복잡한 인자 파서
  private def parseArgs(args: Array[String]): Option[Config] = {
    if (args.length < 5) return None // "worker", "master", "-I", "in", "-O", "out"

    try {
      var masterAddress = ""
      var inputDirs = ListBuffer[String]()
      var outputDir = ""

      var i = 0
      masterAddress = args(i)
      i += 1

      if (args(i) != "-I") return None
      i += 1

      // -I 다음 인자들 (~ -O 전까지)
      while (i < args.length && args(i) != "-O") {
        inputDirs += args(i)
        i += 1
      }

      if (i == args.length || args(i) != "-O" || i + 1 == args.length) return None
      i += 1
      outputDir = args(i)

      if (inputDirs.isEmpty) return None

      // (project.sorting.2025.pptx) 디렉토리 유효성 검사
      inputDirs.foreach(dir =>
        if (!new File(dir).isDirectory) {
          System.err.println(s"Input directory not found: $dir")
          return None
        }
      )
      if (!new File(outputDir).isDirectory) {
        System.err.println(s"Output directory not found: $outputDir")
        return None
      }

      Some(Config(masterAddress, inputDirs.toList, outputDir))

    } catch {
      case _: Exception => None
    }
  }
}