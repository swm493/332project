package worker

import java.io.File
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import utils.Logging

/**
 * WorkerApp: 워커를 실행하는 메인 프로그램
 * worker <master IP:port> -I <input dir1> ... -O <output dir> [-d | --debug]
 */
object WorkerApp {

  private case class Config(
                     masterAddress: String = "",
                     inputDirs: List[String] = List.empty,
                     outputDir: String = ""
                   )

  def main(args: Array[String]): Unit = {

    // 디버그 옵션 확인 및 설정
    val debugArgIndex = args.indexWhere(arg => arg == "-d" || arg == "--debug")
    if (debugArgIndex != -1) {
      Logging.debugMode = true
      println("[Worker] Debug mode enabled.")
    } else {
      Logging.debugMode = false
      println("[Worker] Debug mode disabled.")
    }

    // 디버그 옵션을 제외한 인자들만 파싱
    val filteredArgs = args.filterNot(arg => arg == "-d" || arg == "--debug")

    val config = parseArgs(filteredArgs)
    if (config.isEmpty) {
      System.err.println("Usage: worker <master IP:port> -I <input dir1> [<input dir2> ...] -O <output dir> [-d | --debug]")
      System.exit(1)
    }

    val conf = config.get
    
    val workerNode = new WorkerNode(
      conf.masterAddress,
      conf.inputDirs,
      conf.outputDir
    )

    workerNode.start()
  }

  private def parseArgs(args: Array[String]): Option[Config] = {
    if (args.length < 5) return None

    try {
      var masterAddress = ""
      val inputDirs = ListBuffer[String]()
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