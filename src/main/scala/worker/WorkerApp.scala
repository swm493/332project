package worker

import scala.concurrent.ExecutionContext.Implicits.global
import java.nio.file.Paths

object WorkerApp {
  def main(args: Array[String]): Unit = {

    // 슬라이드에 명시된 커맨드 라인 인자 파싱 (간략화된 버전)
    // worker <master IP:port> -I <input dir> ... -O <output dir>
    if (args.length < 4 || args(1) != "-I" || args(args.indexOf("-O")) != "-O") {
      System.err.println("Usage: worker <master IP:port> -I <input dir1> [input dir2] ... -O <output dir>")
      System.exit(1)
    }

    val masterAddress = args(0)

    val oIndex = args.indexOf("-O")
    val inputDirs = args.slice(2, oIndex).map(Paths.get(_))
    val outputDir = Paths.get(args(oIndex + 1))

    val workerPort = 0 // 0으로 설정 시 자동 할당

    val workerNode = new WorkerNode(workerPort, masterAddress, inputDirs, outputDir)

    workerNode.start()
    workerNode.awaitTermination()
  }
}