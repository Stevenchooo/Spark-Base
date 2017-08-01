package sheshou.streaming

/**
  * Created by steven on 2017/7/17.
  */
class Stats(sqlcontext: SQLContext, dataFrame: DataFrame, cons: GetMysqlConnection) {

  val log = Logger.getLogger("Stats")

  val hoursql = "select sum(1) as count, DATE_FORMAT(concat(year,'-',month,'-',day,' ',hour),'YYYY-MM-dd HH:mm:ss') as time_hour from statics where intrusion = 'mal_operation' group by year,month,day,hour"

  val daysql = "select sum(1) as count, DATE_FORMAT(concat(year,'-',month,'-',day),'YYYY-MM-dd') as time_day from statics where intrusion = 'mal_operation' group by year,month,day"

  val inserthourprefix = "insert into hourly_stat (time_hour, mal_operation) values("
  val inserthourlast = ") ON DUPLICATE KEY UPDATE mal_operation = mal_operation +"

  val insertdayprefix = "insert into dayly_stat (time_day, mal_operation) values("
  val insertdaylast = ") ON DUPLICATE KEY UPDATE mal_operation = mal_operation +"

  def runhourstat(): Unit = {

    val con = cons.getCon()
    con.setAutoCommit(false)
    dataFrame.printSchema()
    println(dataFrame.count())

    if (dataFrame.count() > 0) {
      val hourstatics = sqlcontext.sql(hoursql)

      val hoursat = con.createStatement()

      val hourinsert = hourstatics.collect()

      for (info <- hourinsert) {
        var count = info.get(0)
        var timedate = info.get(1)
        var sql = inserthourprefix + " '" + timedate + "' , " + count + " " + inserthourlast + " " + count
        log.info("hour batch sql is " + sql)
        hoursat.addBatch(sql)

      }
      hoursat.executeBatch()

      con.commit()
      hoursat.close()
      con.close()
    }
  }

  def rundaystat(): Unit = {

    val con = cons.getCon()
    con.setAutoCommit(false)
    dataFrame.printSchema()

    if (dataFrame.count() > 0) {
      val daytatics = sqlcontext.sql(daysql)

      val daystat = con.createStatement()

      val dayinsert = daytatics.collect()

      for (info <- dayinsert) {
        var count = info.get(0)
        var timedate = info.get(1)
        var sql = insertdayprefix + " '" + timedate + "' , " + count + " " + insertdaylast + " " + count
        log.info("daystat batch sql is " + sql)
        daystat.addBatch(sql)
      }
      daystat.executeBatch()

      con.commit()
      daystat.close()
      con.close()
    }
  }

}
