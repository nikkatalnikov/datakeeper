package it_tests

import com.typesafe.config.ConfigFactory
import datakeeper.dirtypartitioner.DirtyPartitionerConfig
import datakeeper.kafkacontext.{KafkaContextConfig, TopicsContext}
import it_tests.utils.PrestoService
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.{Assertion, BeforeAndAfterAll, Matchers, Suite}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

trait TestHelper extends BeforeAndAfterAll with Matchers {
  this: Suite =>

  private val defaultConfig = ConfigFactory.parseResources("dirtypartitioner.conf")
  private val itConfig = ConfigFactory.parseResources("it.conf")
    .withFallback(defaultConfig)
    .resolve()

  private val defaultKafkaConfig = ConfigFactory.parseResources("kafkacontext.conf")
  private val testConfig = ConfigFactory.parseResources("kafkatest.conf")
    .withFallback(defaultKafkaConfig)
    .resolve()

  val topic: String = testConfig.getString("target-topic")

  val kafkaConfig: TopicsContext = KafkaContextConfig(testConfig)(topic)

  val producer = new KafkaProducer[String, GenericRecord](kafkaConfig.kafkaParams.asJava)
  val partitionCount = 3

  private val client = AdminClient.create(kafkaConfig.kafkaParams.asJava)
  private val topicConf = new NewTopic(kafkaConfig.topic, partitionCount, 1)

  override def beforeAll(): Unit = {
    client.createTopics(Set(topicConf).asJava)
  }

  override def afterAll(): Unit = {
    client.deleteTopics(Set(kafkaConfig.topic).asJava)
  }

  val config = DirtyPartitionerConfig(itConfig)

  private val sortColumn = config.sortColumns.head

  def executeHiveStatement = ???

  def getDecimal: String =
    PrestoService
      .execQuery(s"SELECT amount_payed FROM hive.${config.hiveTable} as t", x => x.getString(1))
      .head

  def checkRow(columns: Map[String, String], f1: String, f2: String, v: String): Assertion =
    columns should contain theSameElementsAs Seq(
      "id" -> f1,
      "group_ip" -> f2,
      config.partitionVersionColumn -> v)

  def executeStatement(sql: String): Boolean =
    PrestoService.execStatement(sql)

  def executeQuery(sql: String): List[Map[String, String]] =
    PrestoService.execQuery(sql, x => Map(
      "id" -> x.getString(1),
      "group_ip" -> x.getString(14),
      config.partitionVersionColumn -> x.getString(15)))

  def readTable(): List[Map[String, String]] =
    executeQuery(s"SELECT * FROM hive.${config.hiveTable} as t ORDER BY $sortColumn")

  def readTable(whereClause: String): List[Map[String, String]] =
    executeQuery(s"SELECT * FROM hive.${config.hiveTable} as t WHERE $whereClause ORDER BY $sortColumn")

  def produceRecords(records: TestClass*): Unit = {
    val fs = records
      .map(record => producer.send(new ProducerRecord(kafkaConfig.topic, record.toAvroRecord)))
      .map(f => Future { f.get() })

    Await.ready(Future.sequence(fs), 30 second)
  }
}
