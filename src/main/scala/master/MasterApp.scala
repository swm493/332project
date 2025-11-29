package master

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

/**
 * MasterApp: 마스터를 실행하는 메인 프로그램
 * (project.sorting.2025.pptx - "master <# of workers>")
 */
object MasterApp {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Usage: master <# of workers>")
      System.exit(1)
    }

    val numWorkers = try {
      args(0).toInt
    } catch {
      case _: NumberFormatException =>
        System.err.println("Error: <# of workers> must be an integer.")
        System.exit(1)
        0 // 도달하지 않음
    }

    if (numWorkers <= 0) {
      System.err.println("Error: <# of workers> must be positive.")
      System.exit(1)
    }

    println(s"Starting master, waiting for $numWorkers workers...")

    // gRPC 서버를 위한 스레드 풀
    val execContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(numWorkers + 1))
    
    val masterNode = new MasterNode(execContext, numWorkers)
    masterNode.start()
  }
}