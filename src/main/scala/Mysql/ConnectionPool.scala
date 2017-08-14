package sheshou.mysql

import java.sql.Connection

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.slf4j.LoggerFactory

/**
  * Created by steven on 2017/7/17.
  */
object ConnectionPool {

  val logger = LoggerFactory.getLogger(this.getClass)

  val JDBC_URL = "jdbc:mysql://192.168.1.22/spark"
  val MYSQL_USER = "root"
  val MYSQL_PASSWORD = "root"

  // connection pool settings
  private val connectionPool: Option[BoneCP] = {
    try {
      Class.forName("com.mysql.jdbc.Driver")
      val config = new BoneCPConfig()
      config.setJdbcUrl(JDBC_URL)
      config.setUsername(MYSQL_USER)
      config.setPassword(MYSQL_PASSWORD)
      config.setLazyInit(true)
      config.setMinConnectionsPerPartition(3)
      config.setMaxConnectionsPerPartition(5)
      config.setPartitionCount(5)
      config.setCloseConnectionWatch(true)
      config.setLogStatementsEnabled(false)
      Some(new BoneCP(config))
    } catch {
      case exception: Exception =>
        logger.warn("Create Connection Error: \n" + exception.printStackTrace())
        None
    }
  }

  def getConnection: Option[Connection] = {
    connectionPool match {
      case Some(connPool) => Some(connPool.getConnection)
      case None => None
    }
  }

  def closeConnection(connection: Connection): Unit = {
    if (!connection.isClosed) {
      connection.close()
    }
  }


  def main(args: Array[String]): Unit = {
    getConnection
    println("==========connected successfully!==============")
  }
}
