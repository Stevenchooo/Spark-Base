/**
  * Created by steven on 2017/7/2.
  */
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._

object TestSparkStreaming {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("sheshou").setMaster("local[2]")
    // Create a StreamingContext with a local master
    val ssc = new StreamingContext(sparkconf, Seconds(1))

    // Create a DStream that will connect to serverIP:serverPort, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print a few of the counts to the console
    wordCounts.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
