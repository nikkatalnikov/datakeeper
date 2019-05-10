package datakeeper.mssqlsink

import com.typesafe.config.Config
import com.microsoft.azure.sqldb.spark.config.{Config => MSSQLConf}

object MssqlSinkConfig {

  def apply(config: Config): MSSQLConf = {

    val mssqlConfigMap = Map(
      "url"             -> config.getString("sqlserver.db.urlForSpark"),
      "driver"          -> config.getString("sqlserver.db.driver"),
      "databaseName"    -> config.getString("sqlserver.db.databaseName"),
      "user"            -> config.getString("sqlserver.db.user"),
      "password"        -> config.getString("sqlserver.db.password"),
      "connectTimeout"  -> config.getString("sqlserver.db.connectionTimeout"),
      "dbTable"         -> config.getString("sqlserver.db.dbTable")
    )

    MSSQLConf(mssqlConfigMap)
  }
}
