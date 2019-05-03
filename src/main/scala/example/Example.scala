package example

import com.typesafe.config.{Config, ConfigFactory}
import datakeeper.DataKeeper._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Example {
  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.parseResources("application.conf").resolve()

    val sparkConf: SparkConf = new SparkConf()
      .setAppName(config.getString("spark.app.name"))
      .set("spark.serializer", config.getString("spark.serializer"))

    implicit val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    spark
      .readFromKafka()
      .saveAsDirtyPartition()

    spark.commitOffset()
  }
}
