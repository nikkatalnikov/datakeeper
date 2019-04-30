package it_tests

import java.math.BigDecimal
import java.nio.ByteBuffer

import org.apache.avro.generic.GenericRecord
import org.apache.avro.{RandomData, Schema}

import scala.io.Source

case class TestClass(id: Long, groupId: Int) {

  private val source = Source.fromURL(getClass.getResource("/test_schema.avsc"))
  val schema: Schema = new Schema.Parser().parse(source.mkString)
  source.close()

  def toAvroRecord: GenericRecord = {
    val randomData = new RandomData(schema, 1).iterator.next.asInstanceOf[GenericRecord]
    randomData.put("id", id)
    randomData.put("group_id", groupId)
    randomData.put("amount_payed", serializeDecimal(new BigDecimal("100.01")))

    println(s"randomData: $randomData")
    randomData
  }

  private def serializeDecimal(value: BigDecimal): ByteBuffer =
    ByteBuffer.wrap(value.unscaledValue.toByteArray)
}
