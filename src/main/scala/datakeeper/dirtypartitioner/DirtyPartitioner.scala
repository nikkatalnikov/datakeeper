package datakeeper.dirtypartitioner

import java.util.concurrent.Executors

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, _}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent._
import scala.concurrent.duration.{Duration, DurationDouble}

object DirtyPartitioner {

  implicit class DirtyPartitionDF(df: DataFrame)(implicit spark: SparkSession) extends Serializable {
    type PartitionKey = Map[String, Any]

    @transient
    lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

    private val defaultConfig: Config = ConfigFactory.parseResources("dirtypartitioner.conf").resolve()
    val config: DirtyPartitionerConfig = DirtyPartitionerConfig(defaultConfig)

    private val partitionVersionColumn = config.partitionVersionColumn

    private def getHiveTableSchema(dbName: String, table: String, includePartitions: Boolean = false): StructType = {
      import spark.implicits._
      val fullHiveSchema = spark.table(s"$dbName.$table").schema

      if (includePartitions) {
        fullHiveSchema
      } else {
        val partitions =
          spark.catalog.listColumns(dbName, table).filter(_.isPartition).map(_.name).collect().map(_.toLowerCase())
        StructType(fullHiveSchema.filterNot {
          case StructField(name, _, _, _) => partitions.contains(name.toLowerCase)
        })
      }
    }

    val hiveSchema: StructType = getHiveTableSchema(config.hiveDb, config.hiveTablename, includePartitions = true)
    logger.info(s"Hive schema = $hiveSchema")

    private def partitionPathWithOutVersion(key: PartitionKey, tablePath: String) =
      s"$tablePath/${partitionPathWithOutVersionAndTablePrefix(key)}/"

    private def partitionPathWithOutVersionAndTablePrefix(key: PartitionKey) =
      key.map { case (c, v) => s"$c=$v" }.mkString("/")

    private case class PartitionInfo(key: PartitionKey, tablePath: String, version: Int) {
      def dir: Path =
        new Path(partitionPathWithOutVersion(key, tablePath)).suffix(s"/$partitionVersionColumn=$version")
    }

    def saveAsDirtyPartition(idColumns: Seq[String] = config.idColumns): Unit = {
      val distinctRecords = df.dropDuplicates(idColumns).cache()
      logger.info(s"distinctRecords, ${distinctRecords.show}")

      val partitions = getPartitions(distinctRecords, config.partitioningColumns)
      logger.info(partitions.mkString(s"Input partitions (${partitions.length}): ", ", ", ""))

      val existingPartitions = getExistingPartitions(spark, config.tableDir, config.hiveTable, partitions)
      logger.info(existingPartitions.mkString(s"Existing partitions (${existingPartitions.length}): ", ", ", ""))

      updateExistingPartitions(spark, config, distinctRecords, existingPartitions)
      addNewPartitions(spark, config, distinctRecords, partitions, existingPartitions, hiveSchema)
      logger.info(s"Data successfully saved to ${config.tableDir}")
    }

    private def hiveDdlRepr(key: PartitionKey, version: Int): String =
      (key + (partitionVersionColumn -> version)).map { case (k, v) => s"$k='$v'" }.mkString(", ")

    private def execute(spark: SparkSession, statement: String) = {
      logger.info(s"Executing $statement")
      spark.sql(statement)
    }

    private def addNewPartitions(
                                  spark: SparkSession,
                                  config: DirtyPartitionerConfig,
                                  incomingData: DataFrame,
                                  partitions: Seq[PartitionKey],
                                  existingPartitions: Seq[PartitionInfo],
                                  hiveSchema: StructType): Unit = {
      val newPartitions = partitions.filterNot(p => existingPartitions.exists(_.key == p))

      if (newPartitions.isEmpty) logger.info("No new partitions arrived")
      else {
        logger.info(newPartitions.mkString(s"Saving new partitions (${newPartitions.length}) (", ", ", ")"))

        val newData =
          if (newPartitions.size == partitions.size) incomingData
          else incomingData.filter(row => newPartitions.exists(hasKey(_, row)))

        val hiveCompatibleData = reorderColumns(newData.withColumn(partitionVersionColumn, lit(1)), hiveSchema.fieldNames)

        hiveCompatibleData
          .coalesce(config.numOfOutputFiles)
          .sortWithinPartitions(config.sortColumns.map(col): _*)
          .write
          .partitionBy(config.partitioningColumns :+ partitionVersionColumn: _*)
          .mode(SaveMode.Append)
          .format(config.format)
          .save(config.tableDir)

        newPartitions.foreach { key =>
          execute(spark, s"ALTER TABLE ${config.hiveTable} ADD PARTITION (${hiveDdlRepr(key, 1)})")
        }
      }
    }

    private def updateExistingPartitions(
                                          spark: SparkSession,
                                          config: DirtyPartitionerConfig,
                                          incomingData: DataFrame,
                                          partitions: Seq[PartitionInfo]): Unit = {

      def updatePartition(part: PartitionInfo): Unit = {
        logger.info(s"Updating existing partition ${part.key} located at ${part.dir}")
        logger.info("Merging new and existing data...")
        val incomingRecords = incomingData.filter(hasKey(part.key, _))
        val existingRecords = loadPartition(spark, part.dir, config.tableDir, config.format)
        merge(incomingRecords, existingRecords.drop(partitionVersionColumn), config.idColumns) match {
          case None =>
            logger.info(s"Partition ${part.key} has all the incoming data already")
          case Some(merged) =>
            logger.info(s"Saving merged dataset for partition ${part.key}...")

            merged
              .drop(config.partitioningColumns: _*)
              .coalesce(config.numOfOutputFiles)
              .sortWithinPartitions(config.sortColumns.map(col): _*)
              .write
              .mode(SaveMode.Overwrite)
              .format(config.format)
              .save(s"${part.dir.getParent.toUri.toString}/$partitionVersionColumn=${part.version + 1}")

            execute(spark, s"ALTER TABLE ${config.hiveTable} ADD PARTITION (${hiveDdlRepr(part.key, part.version + 1)})")
            try {
              execute(spark, s"ALTER TABLE ${config.hiveTable} DROP PARTITION (${hiveDdlRepr(part.key, part.version)})")
            } catch {
              case ae: AnalysisException =>
                // Most likely partition was no register or dropped because of a previous error
                logger.error("Exception occurred during partition removing", ae)
            }
        }
      }

      if (partitions.isEmpty) {
        logger.info("Nothing to update")
      } else {
        val parallelism = config.partitionsParallelism.getOrElse(partitions.length)
        logger.info(s"Start updating partitions in parallel with $parallelism executors")
        // move 24.hours to config.timeout in .conf
        executeInParallel(partitions.map(part => () => updatePartition(part)), parallelism, 24.hours)
      }
    }

    private def hasKey(key: PartitionKey, row: Row) =
      key.forall {
        case (field, value) =>
          row.getAs[Any](field) == value
      }

    private def getPartitions(inputDataFrame: DataFrame, partitioningColumns: Seq[String]): Array[PartitionKey] =
      inputDataFrame
        .select(partitioningColumns.map(col): _*)
        .distinct()
        .collect()
        .map(_.getValuesMap[Any](partitioningColumns))

    private def getExistingPartitions(
                                       spark: SparkSession,
                                       tableDir: String,
                                       hiveTable: String,
                                       partitionsFromKafka: Array[PartitionKey]): Seq[PartitionInfo] = {

      def dropPartitionVersionFromPath(path: String) = path.substring(0, path.indexOf(s"/$partitionVersionColumn"))

      def partitionVersion(path: String) =
        path.substring(path.indexOf(s"/$partitionVersionColumn=") + partitionVersionColumn.length + 2).toInt

      val partitionsByPath = partitionsFromKafka.map(p => partitionPathWithOutVersionAndTablePrefix(p) -> p).toMap
      spark
        .sql(s"SHOW PARTITIONS $hiveTable")
        .collect()
        .map(_.getString(0))
        .filter(partitionPathWithVersion =>
          partitionsByPath.keySet(dropPartitionVersionFromPath(partitionPathWithVersion)))
        .map { partitionKeysWithPartitionVersion =>
          PartitionInfo(
            partitionsByPath(dropPartitionVersionFromPath(partitionKeysWithPartitionVersion)),
            tableDir,
            partitionVersion(partitionKeysWithPartitionVersion)
          )
        }
    }

    private def loadPartition(spark: SparkSession, dir: Path, baseOutDir: String, format: String): DataFrame =
      spark.read.option("basePath", baseOutDir).format(format).load(dir.toString)

    private def executeInParallel(actions: Seq[() => Unit], parallelism: Int, timeout: Duration): Unit = {
      implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(parallelism))
      val f = actions.map(f => Future(f()))
      Await.result(Future.sequence(f), timeout)
      ec.shutdown()
    }

    private def merge(incomingData: DataFrame, existingData: DataFrame, idColumns: Seq[String]): Option[DataFrame] = {
      val newData = incomingData.join(existingData, idColumns, "left_anti").cache()
      if (newData.head(1).isEmpty) None
      else Some(reorderColumns(newData, existingData.columns).union(existingData))
    }

    private def reorderColumns(df: DataFrame, columns: Seq[String]): DataFrame =
      df.select(columns.head, columns.tail: _*)
  }
}
