package common

import com.google.protobuf.ByteString
import java.nio.ByteBuffer

// 프로젝트 전반에서 사용할 상수
object Constants {
  val RECORD_LENGTH = 100
  val KEY_LENGTH = 10
  val VALUE_LENGTH = 90
}

/**
 * 10바이트 키를 감싸는 클래스.
 * 바이트 배열을 직접 비교(unsigned)하기 위한 Ordered 트레이트를 구현합니다.
 */
case class Key(bytes: ByteString) extends Ordered[Key] {
  require(bytes.size() == Constants.KEY_LENGTH, s"Key must be ${Constants.KEY_LENGTH} bytes")

  override def compare(that: Key): Int = {
    val b1 = this.bytes.toByteArray
    val b2 = that.bytes.toByteArray

    for (i <- 0 until Constants.KEY_LENGTH) {
      // 바이트를 unsigned int로 변환하여 비교
      val c1 = b1(i) & 0xFF
      val c2 = b2(i) & 0xFF
      if (c1 < c2) return -1
      if (c1 > c2) return 1
    }
    0 //
  }
}

/**
 * 100바이트 레코드를 감싸는 클래스.
 */
case class Record(key: Key, value: ByteString) {
  require(value.size() == Constants.VALUE_LENGTH, s"Value must be ${Constants.VALUE_LENGTH} bytes")

  def toByteArray: Array[Byte] = {
    key.bytes.concat(value).toByteArray
  }
}

object Record {
  /**
   * 100바이트 배열로부터 Record 객체를 생성합니다.
   */
  def fromByteArray(arr: Array[Byte]): Record = {
    require(arr.length == Constants.RECORD_LENGTH, s"Record must be ${Constants.RECORD_LENGTH} bytes")
    val keyBytes = ByteString.copyFrom(arr, 0, Constants.KEY_LENGTH)
    val valueBytes = ByteString.copyFrom(arr, Constants.KEY_LENGTH, Constants.VALUE_LENGTH)
    Record(Key(keyBytes), valueBytes)
  }
}

// 마스터와 워커의 상태를 정의
enum WorkerState {
  case REGISTERED
  case SAMPLING
  case PARTITIONING
  case SHUFFLING
  case MERGING
  case DONE
}