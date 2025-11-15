import GenericInPlaceQuickSort.sort

val testArray = Array(
  Record(5, "apple"),
  Record(1, "banana"),
  Record(8, "cherry"),
  Record(3, "date")
)

implicit val recordOrdering: Ordering[Record] = Ordering.by(_.key)

println(s"원본 Array:\n  ${testArray.mkString("\n  ")}")

sort(testArray)

println(s"\n'key' 기준 정렬된 Array:\n  ${testArray.mkString("\n  ")}")