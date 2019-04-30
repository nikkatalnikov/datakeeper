package it_tests

import com.typesafe.config.ConfigFactory
import it_tests.utils.SparkController
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest._
import org.scalatest.concurrent.Eventually

class DataKeeperTest extends FlatSpec with Eventually with Matchers with Inside with TestHelper {
  private val itConfig = ConfigFactory.load("it.conf")
  private val SparkAppDeployment = itConfig.getString("spark.crd")

  private val fs = FileSystem.get(new java.net.URI(config.fileSystemUrl), new Configuration())
  private val allPartitionColmns = config.partitioningColumns :+ config.partitionVersionColumn

  val sparkController = new SparkController(
    "default",
    SparkAppDeployment
  )

  def runApp(): Unit = {
    sparkController.launchSparkTestDeployment()
    sparkController.cleanUpSparkTestDeployment()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    fs.delete(new Path(config.tableDir), true)
    fs.mkdirs(new Path(config.tableDir))

    executeStatement(s"CREATE SCHEMA IF NOT EXISTS hive.${config.hiveDb}")
    executeStatement(s"DROP TABLE IF EXISTS hive.${config.hiveTable}")

    executeStatement(s"""
      |CREATE TABLE hive.${config.hiveTable} (
      |  id bigint,
      |  has_account boolean,
      |  match_precision_float real,
      |  match_precision_double double,
      |  account_level integer,
      |  last_visited timestamp,
      |  amount_payed decimal(8,2),
      |  registration_date date,
      |  fingertips varbinary,
      |  friends_ids array<bigint>,
      |  preferences map<varchar,varchar>,
      |  personal_record row(name varchar,surname varchar,email varchar),
      |  country_code varchar,
      |  group_id integer,
      |  ${config.partitionVersionColumn} varchar
      |)
      |WITH (
      |  format = 'PARQUET',
      |  partitioned_by = ARRAY[${allPartitionColmns.map(x => s"'${x.mkString}'").mkString(",")}],
      |  external_location = 'hdfs://namenode:8020${config.tableDir}'
      |)
    """.stripMargin)
  }

  it should "read messages from kafka and store to hdfs" in {
    produceRecords(TestClass(1, 1), TestClass(2, 2), TestClass(3, 2), TestClass(4, 4))

    runApp()

    fs.exists(new Path(s"${config.tableDir}/group_id=1")) shouldBe true
    fs.exists(new Path(s"${config.tableDir}/group_id=2")) shouldBe true
    fs.exists(new Path(s"${config.tableDir}/group_id=4")) shouldBe true

    inside(readTable()) {
      case Seq(r1, r2, r3, r4) =>
        checkRow(r1, "1", "1", "1")
        checkRow(r2, "2", "2", "1")
        checkRow(r3, "3", "2", "1")
        checkRow(r4, "4", "4", "1")
    }
  }
}
