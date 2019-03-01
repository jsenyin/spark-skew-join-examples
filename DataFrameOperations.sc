val list = Array(1, 3, 4, 5, 2)
list foreach println

val map = Map("Rock" -> 27, "Spark" -> 5)

for ((k, _) <- map) {println("key is " + k)}

//for((k,_)<-map){println("key is " + k+":"+_)}


val res = for (elem <- list) yield 2 * elem

for (elem <- list if elem % 2 == 0) yield 2 * elem

val c = list.filter(_ % 2 == 0).map(2 * _)

list.sorted
scala.util.Sorting.quickSort(list)

list.mkString(" and ")
val e = list.mkString("<",",",">")

val f = (1,2,3,4,5)
type f