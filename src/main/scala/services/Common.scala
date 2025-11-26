package services

/**
 * 프로젝트 전체에서 사용되는 공통 타입과 상태를 정의합니다.
 */

// 10 바이트 키 (project.sorting.2025.pptx)
type Key = Array[Byte]

// 워커 ID (예: "192.168.0.1:8081")
type WorkerID = String

object Constant {
  object Size {
    def key: Int = 10
    def value: Int = 90
    def record: Int = 100
    def partitionPerWorker: Int = 10
  }
  object Sample {
    def n: Int = 1000
  }
}
object CompareKey {
  // compareKey: Key(byte array) 비교 (unsigned 바이트 비교)
   def compareKey(a: Key, b: Key): Int = {
    val minLen = math.min(a.length, b.length)
    var i = 0
    while (i < minLen) {
      val diff = (a(i) & 0xff) - (b(i) & 0xff)
      if (diff != 0) return diff
      i += 1
    }
    a.length - b.length
  }
}

object RecordOrdering {
  implicit val ordering: Ordering[Array[Byte]] = new Ordering[Array[Byte]] {
    override def compare(x: Array[Byte], y: Array[Byte]): Int = {
      var i = 0
      while (i < Constant.Size.key) {
        val a = x(i) & 0xFF
        val b = y(i) & 0xFF

        if (a != b) {
          return a - b
        }
        i += 1
      }
      0
    }
  }
}

// MasterNode.scala 프로토타입을 기반으로 한 워커 상태
object WorkerState extends Enumeration {
  type WorkerState = Value

  /**
   * Unregistered: 마스터에 등록되기 전
   * Sampling: (Phase 1) 입력 파일에서 샘플을 추출하는 단계
   * Shuffling: (Phase 2) 샘플 제출 후, 셔플 및 파티션을 대기/수행하는 단계
   * Merging: (Phase 3) 셔플된 데이터를 K-way merge 하는 단계
   * Done: 모든 작업 완료
   * Failed: 작업 중 실패 (마스터가 감지)
   */
  val Unregistered, Sampling, Shuffling, Merging, Done, Failed, Waiting = Value
}