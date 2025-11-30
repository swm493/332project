package services

import java.util.PriorityQueue

object MergeService {

  /**
   * 바이트 배열 비교 로직 (Unsigned Lexicographical)
   * * Scala/Java의 Byte는 Signed(-128 ~ 127)이므로,
   * 사전 순서 정렬을 위해 & 0xFF 연산을 통해 Unsigned(0 ~ 255)로 변환하여 비교합니다.
   */
  object RecordOrdering extends Ordering[(Array[Byte], Int)] {
    override def compare(x: (Array[Byte], Int), y: (Array[Byte], Int)): Int = {
      val (bytesA, _) = x
      val (bytesB, _) = y

      var i = 0
      while (i < services.Constant.Size.key) {
        val a = bytesA(i) & 0xFF
        val b = bytesB(i) & 0xFF
        if (a != b) return a - b
        i += 1
      }
      0
    }
  }

  /**
   * K-way Merge 알고리즘
   * * @param inputs: 여러 개의 정렬된 데이터 소스 (Iterator 리스트)
   * @param writeOutput: 정렬된 레코드를 한 건씩 처리할 콜백 함수 (예: 파일 쓰기)
   */
  def merge(inputs: Seq[Iterator[Array[Byte]]], writeOutput: Array[Byte] => Unit): Unit = {
    // 1. 데이터가 남아있는 유효한 이터레이터만 필터링하고, 원본 인덱스를 부여합니다.
    val activeIterators = inputs.zipWithIndex.filter(_._1.hasNext).toArray

    if (activeIterators.isEmpty) return

    // 2. Java PriorityQueue 생성 (Min-Heap)
    val pq = PriorityQueue[(Array[Byte], Int)](activeIterators.length, RecordOrdering)

    // 3. 초기화: 각 이터레이터에서 첫 번째 레코드를 꺼내 큐에 삽입
    for (i <- activeIterators.indices) {
      val (iter, _) = activeIterators(i)
      if (iter.hasNext) {
        // 큐에는 (데이터, activeIterators 배열 내의 인덱스)를 저장
        pq.add((iter.next(), i))
      }
    }

    // 4. 병합 루프
    while (!pq.isEmpty) {
      // 가장 작은 레코드를 꺼냄
      val (minRecord, activeIdx) = pq.poll()

      // 결과 출력 (콜백 호출)
      writeOutput(minRecord)

      // 해당 레코드가 나온 이터레이터에서 다음 값을 가져와 큐에 다시 넣음
      val (iter, _) = activeIterators(activeIdx)
      if (iter.hasNext) {
        pq.add((iter.next(), activeIdx))
      }
    }
  }
}