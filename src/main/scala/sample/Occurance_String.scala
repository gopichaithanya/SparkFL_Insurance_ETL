package sample

object Occurance_String extends App{

  val str = "helloslkhellodjladfjhello"
  val findStr = "hello"
  var lastIndex = 0
  var count = 0

  while ( {
    lastIndex != -1
  }) {
    lastIndex = str.indexOf(findStr, lastIndex)
    if (lastIndex != -1) {
      count += 1
      lastIndex += findStr.length
    }
  }
  System.out.println(count)

}
