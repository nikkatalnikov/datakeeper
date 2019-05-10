package example

import com.typesafe.config.{Config, ConfigFactory}
import datakeeper.dirtypartitioner.DirtyPartitioner._
import datakeeper.kafkacontext.KafkaContext._
import datakeeper.kafkacontext.{KafkaContextConfig, TopicsContext}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Example {
  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.parseResources("application.conf").resolve()
    val topic = config.getString("target-topic")

    val kafkaConfig: Config = ConfigFactory.parseResources("kafkacontext.conf").resolve()
    val topicContext: TopicsContext = KafkaContextConfig(kafkaConfig)(topic)

    val sparkConf: SparkConf = new SparkConf()
      .setAppName(config.getString("spark.app.name"))
      .set("spark.serializer", config.getString("spark.serializer"))

    implicit val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    spark
      .readFromKafka(topicContext)
      .saveAsDirtyPartition()

    spark.commitOffset(topic)
  }
}
