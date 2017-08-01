
/**
  * Created by steven on 2017/7/9.
  */
object TestExceptionConnect {

  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("sheshou").setMaster("local")
    val sparkcontext = new SparkContext(sparkconf)
    val optionbadip = Map("header" -> "true", "path" -> "C:\\Users\\Desktop\\badIp.csv")
    val optiondata = Map("header" -> "true", "path" -> "C:\\Users\\Desktop\\exceptionconnect.csv")
    val sqlcontext = new SQLContext(sparkcontext)
    val badip = sqlcontext.read.options(optionbadip).format("com.databricks.spark.csv").load()
    badip.registerTempTable("badip")
    val data = sqlcontext.read.options(optiondata).format("com.databricks.spark.csv").load()
    data.registerTempTable("exceptiondatas")
    val results = sqlcontext.sql("select exceptiondatas.* from exceptiondatas inner join badip on exceptiondatas.dst_ip = badip.ip").select("id", "analyze_time", "src_ip", "dst_ip", "asset_name", "asset_department", "warning_type", "log_source", "log_id")
    results.show()


  }
}
