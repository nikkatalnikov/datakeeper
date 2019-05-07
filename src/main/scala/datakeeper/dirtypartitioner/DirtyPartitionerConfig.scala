package datakeeper.dirtypartitioner

import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.util.Try

case class DirtyPartitionerConfig(
  fileSystemUrl: String,
  tableDir: String,
  hiveTable: String,
  numOfOutputFiles: Int,
  format: String,
  idColumns: Seq[String],
  partitioningColumns: Seq[String],
  partitionVersionColumn: String,
  sortColumns: Seq[String],
  partitionsParallelism: Option[Int])
{
  val hiveDb :: hiveTablename :: Nil = hiveTable.split('.').toList
}

object DirtyPartitionerConfig {

  def apply(config: Config): DirtyPartitionerConfig = {
    val hiveTable = config.getString("hive.tableName")
    val fileSystemUrl: String = config.getString("fs.url")
    val dir: String = config.getString("fs.dir")
    val format: String = config.getString("fs.format")
    val numOfOutputFiles: Int = config.getInt("fs.numOfOutputFiles")

    val idColumns: Seq[String] = config.getStringList("columns.identity").asScala
    val partitioningColumns: Seq[String] = config.getStringList("columns.partitioning").asScala
    val sortColumns: Seq[String] = config.getStringList("columns.sorting").asScala
    val partitionsParallelism = Try(config.getInt("execution.partitionsParallelism")).toOption

    new DirtyPartitionerConfig(
      fileSystemUrl = fileSystemUrl,
      format = format,
      tableDir = dir,
      numOfOutputFiles = numOfOutputFiles,
      hiveTable = hiveTable,
      partitioningColumns = partitioningColumns,
      partitionVersionColumn = "partition_version",
      idColumns = idColumns,
      sortColumns = sortColumns,
      partitionsParallelism = partitionsParallelism
    )
  }
}
