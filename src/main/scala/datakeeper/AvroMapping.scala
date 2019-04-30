package datakeeper

import java.nio.ByteBuffer
import java.sql.Timestamp

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object AvroMapping {

  implicit class AvroMappingType(s: Schema) {
    def isUnionWith(t: Type): Boolean = s.isUnion && s.getTypes.asScala.exists(_.getType == t)
    def isUnion: Boolean = s.getType == Type.UNION
    def isArray: Boolean = s.getType == Type.ARRAY
    def isRecord: Boolean = s.getType == Type.RECORD
  }

  def convertAvroToSparkValue(value: Any, sparkType: DataType, fieldSchema: Schema): Any =
    (value, sparkType) match {
      case (x: java.lang.Long, TimestampType) => new Timestamp(x)

      case (x: Fixed, _) => x.bytes

      case (x: GenericData.EnumSymbol, StringType) => x.toString

      case (x: Utf8, _) => x.toString

      case (x: ByteBuffer, _: DecimalType) => deserializeDecimal(x, getDecimalScale(fieldSchema))

      case (x: ByteBuffer, BinaryType) => x.array

      case (x: java.util.Collection[_], ArrayType(t, _)) => x
        .map(elem => convertAvroToSparkValue(elem, t, extractInternalSchema(fieldSchema)))
        .toArray

      case (x: GenericRecord, t: StructType) => convertGenericRecord(x, t, fieldSchema)

      case (x: java.util.HashMap[_, _], MapType(StringType, StringType, _)) =>
        x.map {
          case (key, value: CharSequence) => key.toString -> value.toString
          case _ => throw new AssertionError("Unsupported Map value type")
        }

      case (x, _) => x
    }

  private def convertGenericRecord(gr: GenericRecord, structType: StructType, fieldSchema: Schema): Any =
    fieldSchema match {

      case AvroUnionArray(s) =>
        convertAvroToSparkValue(gr, structType, extractInternalSchema(s))

      case AvroRecord(_) =>
        val avroFields = gr.getSchema.getFields
          .map(f => f.name.toLowerCase -> (f.name, f.schema))
          .toMap

        val values = structType.toList.map {
          case StructField(columnName, columnDataType, _, _) =>
            avroFields
              .get(columnName.toLowerCase)
              .map {
                case (avroName, fSchema) =>
                  convertAvroToSparkValue(gr.get(avroName), columnDataType, fSchema)
              }
              .orNull
        }
        new GenericRow(values.toArray[Any])

      case AvroUnion(_) =>
        val values = fieldSchema.getTypes.asScala
          .zip(structType.fields)
          .map {
            case (unionElementSchema, structField) if unionElementSchema == gr.getSchema =>
              convertAvroToSparkValue(gr, structField.dataType, gr.getSchema)
            case _ => null
          }
        new GenericRow(values.toArray[Any])

      case _ =>
        throw new AssertionError(s"Can't cast ${fieldSchema.getType} to structType $structType")
    }

  private def nullableAsRequired(schema: Schema): Option[Schema] =
    schema.getTypes.asScala.find(_.getType != Type.NULL)

  private def extractNestedSchema(s: Schema): Schema =
    if (s.isUnionWith(Type.NULL)) nullableAsRequired(s).get
    else s

  private def getDecimalScale(schema: Schema): Int = {
    val schemaOpt =
      if (schema.isUnionWith(Type.NULL)) nullableAsRequired(schema)
      else Some(schema)

    schemaOpt
      .map(_.getObjectProp("scale").toString)
      .map(Integer.parseInt)
      .get
  }

  private def deserializeDecimal(buffer: ByteBuffer, scale: Int) =
    new java.math.BigDecimal(new java.math.BigInteger(buffer.array), scale)

  private def extractInternalSchema(s: Schema): Schema =
    if (s.isUnionWith(Type.NULL)) {
      assert(s.getTypes.size() == 2, s"$s is a union type with null - only nullable and exclusive unions are supported")
      extractNestedSchema(s)
    } else if (s.isArray) {
      s.getElementType
    } else {
      throw new AssertionError("Only union with NULL or array are supported")
    }

  private case object AvroRecord {
    def unapply(s: Schema): Option[Schema] = if (s.isRecord) Some(s) else None
  }

  private case object AvroUnion {
    def unapply(s: Schema): Option[Schema] = if (s.isUnion) Some(s) else None
  }

  private case object AvroUnionArray {
    def unapply(s: Schema): Option[Schema] = if (s.isUnionWith(Type.NULL) || s.isArray) Some(s) else None
  }
}
