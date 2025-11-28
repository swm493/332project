package worker

import java.io.File
import scala.collection.mutable.ListBuffer

/**
 * WorkerApp: 워커를 실행하는 메인 프로그램
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
    val selfIP = java.net.InetAddress.getLocalHost.getHostAddress
    val selfPort = 8080;
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

      inputDirs.find(dir => !new File(dir).isDirectory) match {
        case Some(invalidDir) =>
          System.err.println(s"Input directory not found: $invalidDir")
          None

        case None =>
          if (!new File(outputDir).isDirectory) {
            System.err.println(s"Output directory not found: $outputDir")
            None
          } else {
            Some(Config(masterAddress, inputDirs.toList, outputDir))
          }
      }

    } catch {
      case _: Exception => None
    }
  }
}