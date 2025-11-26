package services

import java.io.*
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Try, Using}

object StorageService {
  
  def extractSamples(file: File): List[Key] = {
    if(!file.exists() || file.length() == 0){
      return List.empty
    }
    
    val fis = new FileInputStream(file)
    try {
      val maxRecords = (file.length() / Constant.Size.record).toInt
      val recordsToRead = math.min(Constant.Sample.n, maxRecords)
      val bytesToRead = recordsToRead * Constant.Size.record

      if (bytesToRead == 0) return List.empty
      
      val buffer = new Array[Byte](bytesToRead)
      
      val actualRead = fis.read(buffer)
      
      val keys = new ListBuffer[Key]()
      var offset = 0
      
      while (offset + Constant.Size.key <= actualRead) {
        val keyBytes = new Array[Byte](Constant.Size.key)
        
        System.arraycopy(buffer, offset, keyBytes, 0, Constant.Size.key)
        
        keys += keyBytes
        
        offset += Constant.Size.record
      }

      keys.toList
    } finally {
      fis.close()
    }
  }

  /**
   * [Phase 2 & 3] 레코드 읽기
   * 파일에서 100바이트 단위로 레코드를 읽어오는 Iterator를 반환합니다.
   * BufferedInputStream을 사용하여 I/O 성능을 최적화했습니다.
   */
  def readRecords(file: File): Iterator[Array[Byte]] = {
    if (!file.exists()) return Iterator.empty

    // 64KB Buffer
    val bis = new BufferedInputStream(new FileInputStream(file), 64 * 1024)

    new Iterator[Array[Byte]] {
      var nextRecord: Option[Array[Byte]] = fetchNext()
      var isClosed = false

      private def fetchNext(): Option[Array[Byte]] = {
        if (isClosed) return None

        val buffer = new Array[Byte](Constant.Size.record)
        var totalRead = 0

        try {
          while (totalRead < Constant.Size.record) {
            val read = bis.read(buffer, totalRead, Constant.Size.record - totalRead)
            if (read == -1) {
              closeStream()
              return None
            }
            totalRead += read
          }
          Some(buffer)
        } catch {
          case _: IOException =>
            closeStream()
            None
        }
      }

      private def closeStream(): Unit = {
        if (!isClosed) {
          bis.close()
          isClosed = true
        }
      }

      override def hasNext: Boolean = nextRecord.isDefined

      override def next(): Array[Byte] = {
        val r = nextRecord.getOrElse(throw new NoSuchElementException("No more records"))
        nextRecord = fetchNext()
        r
      }
    }
  }

  /**
   * [Phase 3: Merging] 파일 병합 실행기
   * 1. 입력 파일들의 스트림을 엽니다.
   * 2. 출력 파일 스트림을 엽니다.
   * 3. SortingService에게 정렬 로직을 위임합니다.
   */
  def mergeFiles(inputFiles: List[File], outputFile: File): Unit = {
    if (inputFiles.isEmpty) return

    println(s"[Storage] Preparing to merge ${inputFiles.length} files into ${outputFile.getName}...")

    // 1. 입력 스트림(Iterator) 준비
    val inputIterators = inputFiles.map(readRecords)

    // 2. 출력 스트림 준비 (BufferedOutputStream 필수)
    val bos = new BufferedOutputStream(new FileOutputStream(outputFile), 64 * 1024)

    try {
      // 3. SortingService에 '데이터 소스'와 '쓰기 콜백' 전달
      MergeService.merge(
        inputs = inputIterators,
        writeOutput = (record: Array[Byte]) => bos.write(record)
      )

      bos.flush()
      println(s"[Storage] Merge complete: ${outputFile.getPath}")
    } catch {
      case e: Exception =>
        println(s"[Storage] Error during merge: ${e.getMessage}")
        throw e
    } finally {
      // 출력 스트림 닫기
      bos.close()
      // 입력 스트림들은 Iterator 내부에서 EOF 도달 시 닫히지만, 
      // 예외 발생 시 등의 안전을 위해 GC에 의존하거나, 별도의 관리 로직을 추가할 수 있음
    }
  }

  /**
   * [Utility] 셔플 단계에서 받은 임시 파일 목록 조회
   * "shuffle-" 로 시작하는 파일들을 찾습니다.
   */
  def getReceivedTempFiles(outputDir: String): List[File] = {
    val dir = new File(outputDir)
    if (dir.exists && dir.isDirectory) {
      dir.listFiles.filter(f => f.isFile && f.getName.startsWith("shuffle-")).toList
    } else {
      List.empty
    }
  }

  /**
   * [Utility] 임시 파일 삭제
   */
  def deleteTempFiles(files: List[File]): Unit = {
    println(s"[Storage] Deleting ${files.length} temp files...")
    files.foreach(file => {
      try {
        if (file.exists) file.delete()
      } catch {
        case e: SecurityException =>
          println(s"[Storage] Failed to delete ${file.getName}: ${e.getMessage}")
      }
    })
  }

  /**
   * [Utility] 디렉토리 내 모든 파일 조회
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