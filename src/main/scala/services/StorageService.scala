package services

import services.Key
import java.io.File

/**
 * 디스크 I/O 관련 로직을 캡슐화합니다. (worker.scala 프로토타입 기반)
 * WorkerNode가 이 객체를 사용하여 파일을 읽고 씁니다.
 */
object StorageService {

  /**
   * 입력 파일(100바이트 레코드)에서 10바이트 키 샘플을 추출합니다.
   * (worker.scala - executeSampling)
   */
  def extractSamples(file: File): List[Key] = {
    println(s"[Storage] Extracting samples from ${file.getName}...")
    // TODO: 100바이트 레코드 읽고 10바이트 키 추출 로직
    // 예: N개의 레코드 중 M개를 샘플링
    List.empty
  }

  /**
   * 입력 파일의 모든 레코드(100바이트)를 읽는 이터레이터
   * (worker.scala - executeShuffleAndPartition)
   */
  def readRecords(file: File): Iterator[Array[Byte]] = {
    println(s"[Storage] Reading records from ${file.getName}...")
    // TODO: 100바이트 레코드 단위로 읽는 로직
    Iterator.empty
  }

  /**
   * 셔플 단계에서 받은 임시 파일 목록을 가져옵니다.
   * (worker.scala - executeMerge)
   */
  def getReceivedTempFiles(outputDir: String): List[File] = {
    println(s"[Storage] Getting received temp files from $outputDir...")
    // TODO: 셔플로 받은 임시 파일들 반환 (예: "shuffle_temp_*")
    List.empty
  }

  /**
   * K-way merge 수행
   * (worker.scala - executeMerge)
   */
  def mergeSortFiles(files: List[File], outputFile: File): Unit = {
    println(s"[Storage] Merging ${files.length} files into ${outputFile.getName}...")
    // TODO: K-way merge 로직 구현
  }

  /**
   * 임시 파일 삭제
   * (project.sorting.2025.pptx - "delete them in the end")
   */
  def deleteTempFiles(files: List[File]): Unit = {
    println(s"[Storage] Deleting ${files.length} temp files...")
    files.foreach(file => {
      // if (file.exists) file.delete()
    })
  }

  /**
   * 디렉토리의 파일 목록을 가져옵니다.
   */
  def listFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List.empty
    }
  }
}