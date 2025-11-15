package services

import common.{Constants, Record}
import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, FileInputStream, FileOutputStream, File}
import java.nio.file.Path

/**
 * 디스크에서 'gensort' 형식의 레코드를 읽는 클래스
 */
class RecordReader(filePath: Path) extends Iterator[Record] {
  private val dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filePath.toFile)))
  private var nextRecord: Option[Record] = None

  // 첫 번째 레코드를 미리 읽어둠
  readNext()

  private def readNext(): Unit = {
    val buffer = new Array[Byte](Constants.RECORD_LENGTH)
    try {
      val bytesRead = dis.read(buffer)
      if (bytesRead == Constants.RECORD_LENGTH) {
        nextRecord = Some(Record.fromByteArray(buffer))
      } else if (bytesRead == -1) {
        // 파일 끝
        nextRecord = None
        dis.close()
      } else {
        // 비정상적인 파일 (100바이트 배수가 아님)
        throw new RuntimeException(s"Invalid record length read: $bytesRead")
      }
    } catch {
      case e: java.io.EOFException =>
        nextRecord = None
        dis.close()
    }
  }

  override def hasNext: Boolean = nextRecord.isDefined

  override def next(): Record = {
    val current = nextRecord.get
    readNext()
    current
  }
}

/**
 * 디스크에 'gensort' 형식의 레코드를 쓰는 클래스
 */
class RecordWriter(filePath: Path) {
  private val dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filePath.toFile)))

  def write(record: Record): Unit = {
    dos.write(record.toByteArray)
  }

  def writeAll(records: Iterator[Record]): Unit = {
    records.foreach(write)
  }

  def close(): Unit = {
    dos.flush()
    dos.close()
  }
}

// (LocalStorageService 클래스는 일단 보류 - RecordReader/Writer를 직접 사용하는 것이 더 효율적일 수 있음)