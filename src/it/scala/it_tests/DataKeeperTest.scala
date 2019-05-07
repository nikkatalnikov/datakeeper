package it_tests

import com.typesafe.config.ConfigFactory
import it_tests.utils.SparkController
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest._
import org.scalatest.concurrent.Eventually

class DataKeeperTest extends FlatSpec with Eventually with Matchers with Inside with TestHelper {
  private val SparkAppDeployment = ConfigFactory.load("it.conf").getString("spark.crd")

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

  override def afterAll(): Unit = {
    super.afterAll()
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
      case List(r1, r2, r3, r4) =>
        checkRow(r1, "1", "1", "1")
        checkRow(r2, "2", "2", "1")
        checkRow(r3, "3", "2", "1")
        checkRow(r4, "4", "4", "1")
    }
  }

  // TODO: add all logical/complex types
  it should "respect complex and logical datatypes" in {
    getDecimal shouldEqual "100.01"
  }

  it should "remove duplicates from incoming data" in {
    produceRecords(
      TestClass(1, 1), // already existing record
      TestClass(3, 2),
      TestClass(3, 2), // already existing and duplicated
      TestClass(5, 5),
      TestClass(5, 5)) // duplicated but not existing

    runApp()

    inside(readTable()) {
      case List(r1, r2, r3, r4, r5) =>
        checkRow(r1, "1", "1", "1")
        checkRow(r2, "2", "2", "1")
        checkRow(r3, "3", "2", "1")
        checkRow(r4, "4", "4", "1")
        checkRow(r5, "5", "5", "1")
    }
  }

  it should "update existing partitions" in {
    produceRecords(TestClass(6, 1))

    runApp()

    inside(readTable()) {
      case List(r1, r2, r3, r4, r5, r6) =>
        checkRow(r1, "1", "1", "2")
        checkRow(r2, "2", "2", "1")
        checkRow(r3, "3", "2", "1")
        checkRow(r4, "4", "4", "1")
        checkRow(r5, "5", "5", "1")
        checkRow(r6, "6", "1", "2")
    }
  }

//  TODO: use Hive SQL client for executeHiveStatement
//  it should "ignore bad partition-version" in {
//    val path = s"${config.tableDir}/group_id=9/${config.partitionVersionColumn}="
//
//    fs.mkdirs(new Path(path + 9))
//
//    val addPartitionSql = s"""
//      | ALTER TABLE hive.${config.hiveTable}
//      | if not exists add partition (group_id=9, ${config.partitionVersionColumn}=9) LOCATION '${path}9'
//      """.stripMargin
//
//    executeHiveStatement(addPartitionSql)
//    produceRecords(TestClass(10, 9))
//    runApp()
//
//    fs.mkdirs(new Path(path + 10))
//    produceRecords(TestClass(11, 9))
//
//    runApp()
//
//    fs.exists(new Path(path + 9)) shouldBe true
//    fs.exists(new Path(path + 10)) shouldBe true
//
//    inside(readTable("group_id = 9")) {
//      case List(r1, r2) =>
//        checkRow(r1, "10", "9", "10")
//        checkRow(r2, "11", "9", "10")
//    }
//  }

//  TODO: implement once previous is done
//  it should "save new partition even if data come only for one of existing partition" in {
//    produceRecords(TestClass(1, 1), TestClass(6, 1), TestClass(12, 12), TestClass(11, 9))
//
//    runApp()
//
//    inside(readTable()) {
//      case List(r1, r2, r3, r4, r5, r6, r10, r11, r12) =>
//        checkRow(r1, "1", "1", "3")
//        checkRow(r2, "2", "2", "1")
//        checkRow(r3, "3", "2", "1")
//        checkRow(r4, "4", "4", "1")
//        checkRow(r5, "5", "5", "1")
//        checkRow(r6, "6", "1", "3")
//        checkRow(r10, "10", "9", "10")
//        checkRow(r11, "11", "9", "10")
//        checkRow(r12, "12", "12", "1")
//    }
//  }
}
