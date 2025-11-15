object SimpleSort {

  def mergeSort(nums: List[Int]): List[Int] = {

    def merge(left: List[Int], right: List[Int]): List[Int] = (left, right) match {
      case (Nil, r) => r
      case (l, Nil) => l
      case (lHead :: lTail, rHead :: rTail) =>
        if (lHead < rHead) lHead :: merge(lTail, right)
        else rHead :: merge(left, rTail)
    }

    val n = nums.length / 2
    if (n == 0) nums
    else {
      val (left, right) = nums.splitAt(n)
      val sortedLeft = mergeSort(left)
      val sortedRight = mergeSort(right)

      merge(sortedLeft, sortedRight)
    }
  }
}