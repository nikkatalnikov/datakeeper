package it_tests


import datakeeper.kafkacontext.OffsetManager
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{FlatSpec, Matchers}

class OffsetManagerTest extends FlatSpec with Matchers with TestHelper {
  override def afterAll(): Unit = {
    super.afterAll()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  it should "respect initial offsets before writing to topic" in {
    val offsetManager = new OffsetManager(kafkaConfig.kafkaParams, kafkaConfig.topic, kafkaConfig.maxMessages)
    val range = offsetManager.getOffsetRanges()
    range.length shouldBe partitionCount
    range.foreach(_.fromOffset shouldBe 0)
    range.foreach(_.untilOffset shouldBe 0)
    offsetManager.commitOffsets()
  }

  it should "respect offsets after writing to topic" in {
    producer.send(new ProducerRecord(kafkaConfig.topic, TestClass(1, 2).toAvroRecord))
    producer.send(new ProducerRecord(kafkaConfig.topic, TestClass(11, 22).toAvroRecord))
    producer.flush()

    val offsetManager = new OffsetManager(kafkaConfig.kafkaParams, kafkaConfig.topic, kafkaConfig.maxMessages)

    val range = offsetManager.getOffsetRanges()
    range.length shouldBe partitionCount
    range.foreach(_.fromOffset shouldBe 0)
    range.map(_.untilOffset).sum shouldBe 2

    offsetManager.commitOffsets()

    producer.send(new ProducerRecord(kafkaConfig.topic, TestClass(1, 2).toAvroRecord))
    producer.send(new ProducerRecord(kafkaConfig.topic, TestClass(11, 22).toAvroRecord))
    producer.flush()

    val offsetManager2 = new OffsetManager(kafkaConfig.kafkaParams, kafkaConfig.topic, kafkaConfig.maxMessages)

    val range2 = offsetManager2.getOffsetRanges()
    range2.map(_.fromOffset).sum shouldBe 2
    range2.map(_.untilOffset).sum shouldBe 4

    offsetManager2.commitOffsets()
  }

  it should "read all messages with small maxMessages param" in {
    val maxMessagesPerPartition = 2
    val msgCount = 10
    (0 until msgCount).foreach { _ =>
      producer.send(new ProducerRecord(kafkaConfig.topic, TestClass(1, 2).toAvroRecord))
    }
    producer.flush()

    var processedRecordCount = 0
    (0 until msgCount).foreach { _ =>
      val offsetManager = new OffsetManager(kafkaConfig.kafkaParams, kafkaConfig.topic, maxMessagesPerPartition)
      val readRecords = offsetManager.getOffsetRanges().map(offset => offset.untilOffset - offset.fromOffset).sum.toInt
      readRecords should be <= partitionCount * maxMessagesPerPartition
      offsetManager.commitOffsets()
      processedRecordCount += readRecords
    }

    processedRecordCount shouldBe msgCount
  }
}
