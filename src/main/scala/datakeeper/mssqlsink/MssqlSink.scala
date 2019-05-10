package datakeeper.mssqlsink

import com.microsoft.azure.sqldb.spark.config.{Config => MSSQLConf}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import com.microsoft.azure.sqldb.spark.connect._

object MssqlSink {

  implicit class MssqlDF(df: DataFrame)(implicit spark: SparkSession) extends Serializable {
    @transient
    lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)
    val conf: Config = ConfigFactory.parseResources("mssqlsink.conf").resolve()
    val mssqlConf: MSSQLConf = MssqlSinkConfig(conf)

    def saveAsMssqlDF(): Unit = {
      df
        .write
        .mode(SaveMode.Append)
        .sqlDB(mssqlConf)

      logger.info(s"Data successfully saved to ${mssqlConf.get("dbTable")}")
    }
  }

}