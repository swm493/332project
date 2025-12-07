package master

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import utils.Logging

/**
 * MasterApp: 마스터를 실행하는 메인 프로그램
 * Usage: master <# of workers> [-d | --debug]
 */
object MasterApp {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: master <# of workers> [-d | --debug]")
      System.exit(1)
    }

    // 디버그 옵션 확인
    val debugArgIndex = args.indexWhere(arg => arg == "-d" || arg == "--debug")
    if (debugArgIndex != -1) {
      Logging.debugMode = true
      println("[Master] Debug mode enabled.")
    } else {
      Logging.debugMode = false
      println("[Master] Debug mode disabled.")
    }

    val numWorkersStr = if (debugArgIndex == 0) {
      if (args.length > 1) args(1) else ""
    } else {
      args(0)
    }

    val numWorkers = try {
      numWorkersStr.toInt
    } catch {
      case _: NumberFormatException =>
        System.err.println("Error: <# of workers> must be an integer.")
        System.exit(1)
        0
    }

    if (numWorkers <= 0) {
      System.err.println("Error: <# of workers> must be positive.")
      System.exit(1)
    }

    println(s"Starting master, waiting for $numWorkers workers...")

    val execContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(numWorkers + 1))
    val masterNode = new MasterNode(execContext, numWorkers)
    masterNode.start()
  }
}