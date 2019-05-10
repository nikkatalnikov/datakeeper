package datakeeper.kafkacontext

import com.typesafe.config.{Config, ConfigFactory}
import datakeeper.kafkacontext.KafkaContextConfig.KafkaContextConfigMap
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object KafkaContext {

  implicit class KafkaContext(sparkSession: SparkSession) extends Serializable {
    @transient
    lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

    val spark: SparkSession = sparkSession

    private val defaultConfig: Config = ConfigFactory.parseResources("kafkacontext.conf").resolve()
    val config: KafkaContextConfigMap = KafkaContextConfig(defaultConfig)
    val offsetManagersMap: Map[String, OffsetManager] = config.map {
      case (topic: String, context: TopicsContext) => topic -> new OffsetManager(context.kafkaParams, topic, context.maxMessages)
    }

    def readFromKafka(topicContext: TopicsContext): DataFrame = {
      val offsetRanges = offsetManagersMap(topicContext.topic).getOffsetRanges(topicContext.initialOffsets)
      logger.info(offsetRanges.mkString("Offsets to read: ", ", ", ""))
      logger.info(s"Count of messages: ${offsetRanges.map(range => range.untilOffset - range.fromOffset).sum}")

      val kafkaRDD = KafkaUtils.createRDD[String, GenericRecord](
        sc = sparkSession.sparkContext,
        kafkaParams = topicContext.kafkaParams.asJava,
        offsetRanges = offsetRanges,
        locationStrategy = LocationStrategies.PreferConsistent)

      val avroSchema: Schema = kafkaRDD.first.value.getSchema
      logger.info(s"avroSchema: $avroSchema")

      val sparkSchema: StructType = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
      logger.info(s"sparkSchema: $sparkSchema")

      val deserializedRDD: RDD[Row] = kafkaRDD.map[Row](cr =>
        AvroMapping.convertAvroToSparkValue(cr.value, sparkSchema, cr.value.getSchema).asInstanceOf[GenericRow])

      spark.createDataFrame(deserializedRDD, sparkSchema)
    }

    def commitOffset(topic: String): Unit = {
      val offsetManager = offsetManagersMap(topic)
      val committedOffsets = offsetManager.commitOffsets()
      logger.info(s"Offsets committed $committedOffsets")
    }
  }
}