package services

import java.io._
import scala.collection.mutable
import scala.util.{Using, Try}

object StorageService {
  val RECORD_SIZE = 100
  val KEY_SIZE = 10

  /**
   * [Phase 1: Sampling]
   * 입력 파일에서 균등한 간격(Stride)으로 10바이트 키를 샘플링합니다.
   * 파일 전체를 읽지 않고 RandomAccessFile을 사용하여 효율적으로 건너뛰며 읽습니다.
   */
  def extractSamples(file: File): List[Key] = {
    if (!file.exists()) return List.empty

    println(s"[Storage] Extracting samples from ${file.getName}...")
    val samples = mutable.ListBuffer[Key]()

    val fileSize = file.length()
    val numRecords = fileSize / RECORD_SIZE

    // 최대 1000개의 샘플을 추출한다고 가정
    val targetSampleCount = 1000
    // 전체 레코드 수가 1000개 이하면 모든 레코드를 샘플링, 아니면 간격을 둠
    val stride = if (numRecords <= targetSampleCount) 1 else (numRecords / targetSampleCount).toInt
    val loopCount = if (numRecords <= targetSampleCount) numRecords.toInt else targetSampleCount

    Using(new RandomAccessFile(file, "r")) { raf =>
      for (i <- 0 until loopCount) {
        val pos = i.toLong * stride * RECORD_SIZE
        if (pos < fileSize) {
          raf.seek(pos)
          val buffer = new Array[Byte](KEY_SIZE)
          raf.read(buffer) // 키 부분만(10 bytes) 읽음

          // Key 타입이 Array[Byte]이므로 복사본 저장 (안전성 위해 clone 권장되나 여기선 단순화)
          samples += buffer.clone()
        }
      }
    }.getOrElse {
      println(s"[Storage] Failed to extract samples from ${file.getName}")
      List.empty
    }

    samples.toList
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

        val buffer = new Array[Byte](RECORD_SIZE)
        var totalRead = 0

        try {
          while (totalRead < RECORD_SIZE) {
            val read = bis.read(buffer, totalRead, RECORD_SIZE - totalRead)
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