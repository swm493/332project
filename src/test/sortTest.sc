import SimpleSort.mergeSort

println("--- merge sort test ---")

val unsortedList = List(5, 1, 8, 3, 10, 2, 7, 4, 9, 6)
val sortedList = mergeSort(unsortedList)
println(s"original: $unsortedList")
println(s"result: $sortedList")

val unsortedList2 = List(100, 50, -5, 0, 10)
val sortedList2 = mergeSort(unsortedList2)
println(s"original: $unsortedList2")
println(s"result: $sortedList2")