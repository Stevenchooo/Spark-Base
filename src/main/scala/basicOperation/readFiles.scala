package basicOperation

import scala.io.Source

/**
  * Created by cWX491729 on 2017/8/4.
  */
object readFiles {
  def main(args: Array[String]): Unit = {
    val file = Source.fromFile ("1.txt")
    // val file = Source.fromURL( "http://spark.apache.org")
    for (line <- file.getLines()){
      println(line)
    }
    file.close()
  }
}
