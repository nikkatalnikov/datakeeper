package it_tests

import com.typesafe.config.ConfigFactory
import datakeeper.DataKeeperConfig
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

  private val defaultConfig = ConfigFactory.parseResources("datakeeper.conf")
  private val testConfig = ConfigFactory.parseResources("it.conf")
    .withFallback(defaultConfig)
    .resolve()

  val config = DataKeeperConfig(testConfig)

  val producer = new KafkaProducer[String, GenericRecord](config.kafkaParams.asJava)
  val partitionCount = 3

  private val sortColumn = config.sortColumns.head

  private val client = AdminClient.create(config.kafkaParams.asJava)
  private val topicConf = new NewTopic(config.topic, partitionCount, 1)

  override def beforeAll(): Unit = {
    client.createTopics(Set(topicConf).asJava)
  }

  override def afterAll(): Unit = {
    client.deleteTopics(Set(config.topic).asJava)
  }

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
      .map(record => producer.send(new ProducerRecord(config.topic, record.toAvroRecord)))
      .map(f => Future { f.get() })

    Await.ready(Future.sequence(fs), 30 second)
  }
}
