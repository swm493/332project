case class Record(key: Int, value: String)

object GenericInPlaceQuickSort {

  def sort[T](arr: Array[T])(implicit ord: Ordering[T]): Unit = {
    if (arr == null || arr.length == 0) return
    quickSortHelper(arr, 0, arr.length - 1)
  }

  private def quickSortHelper[T](arr: Array[T], low: Int, high: Int)(implicit ord: Ordering[T]): Unit = {
    if (low < high) {
      val pivotIndex = partition(arr, low, high)
      quickSortHelper(arr, low, pivotIndex - 1)
      quickSortHelper(arr, pivotIndex + 1, high)
    }
  }

  private def partition[T](arr: Array[T], low: Int, high: Int)(implicit ord: Ordering[T]): Int = {
    val pivot = arr(high) // 피벗 객체
    var i = low - 1

    for (j <- low until high) {
      if (ord.lteq(arr(j), pivot)) {
        i += 1
        swap(arr, i, j)
      }
    }
    swap(arr, i + 1, high)
    i + 1
  }

  private def swap[T](arr: Array[T], i: Int, j: Int): Unit = {
    val temp = arr(i)
    arr(i) = arr(j)
    arr(j) = temp
  }
}