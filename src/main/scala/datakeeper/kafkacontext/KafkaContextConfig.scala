package datakeeper.kafkacontext

import com.typesafe.config.Config
import org.apache.kafka.common.TopicPartition
import pureconfig.generic.auto._ // important

case class TopicsContext(
                          topic: String,
                          groupId: String,
                          autocommit: Boolean,
                          offsetMode: String,
                          partitionsCount: Int,
                          identity: Set[String],
                          private val maxMessagesPerPartition: Option[Int],
                          private val initialOffsetsMap: Option[Seq[Offset]])(implicit config: Config)
{
  require(partitionsCount != 0)
  require(identity.nonEmpty)

  val kafkaParams: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> config.getString("url"),
    "key.deserializer"          -> config.getString("key.deserializer"),
    "value.deserializer"        -> config.getString("value.deserializer"),
    "key.serializer"            -> config.getString("key.serializer"),
    "value.serializer"          -> config.getString("value.serializer"),
    "schema.registry.url"       -> config.getString("schema.registry.url"),
    "auto.offset.reset"         -> offsetMode,
    "group.id"                  -> groupId,
    "enable.auto.commit"        -> Boolean.box(autocommit)
  )

  val initialOffsets: Map[TopicPartition, Long] = initialOffsetsMap match {
    case Some(c) => c.map(x => new TopicPartition(topic, x.partition) -> x.offset).toMap
    case None => Map.empty
  }

  val maxMessages: Long = maxMessagesPerPartition match {
    case Some(c) => c
    case None => Long.MaxValue
  }

  override def toString: String =
    s"""
       |topic: $topic,
       |groupId: $groupId,
       |autocommit: $autocommit,
       |offsetMode: $offsetMode,
       |partitionsCount: $partitionsCount,
       |identity: $identity,
       |maxMessages: $maxMessages,
       |initialOffsets: $initialOffsets,
       |kafkaParams: $kafkaParams
    """.stripMargin
}

case class Offset(offset: Long, partition: Int)

object KafkaContextConfig {
  type KafkaContextConfigMap = Map[String, TopicsContext]

  def apply(config: Config): KafkaContextConfigMap = {
    implicit val conf: Config = config
    val topicsConf = config.getConfig("topics")
    pureconfig.loadConfigOrThrow[KafkaContextConfigMap](topicsConf).map {
      case (_: String, v: TopicsContext) => v.topic -> v
    }
  }
}
