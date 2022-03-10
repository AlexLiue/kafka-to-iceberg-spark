package org.apache.iceberg.streaming.avro

import org.apache.avro.Schema.Type.{ARRAY, BOOLEAN, BYTES, DOUBLE, ENUM, FIXED, FLOAT, INT, LONG, MAP, NULL, RECORD, STRING, UNION}
import org.apache.avro.{LogicalTypes, Schema}


import java.util

/**
 * Confluent Avro Schema, Fix Logical Type.
 * Debezium 解析的 Avro Schema 信息中没有 Logical Type, 此处添加相关信息。
 * */
object SchemaFixLogicalType {


  def fixLogicalType(avroSchema: Schema): Schema = {
    val copySchema = new Schema.Parser().parse(avroSchema.toString)
    SchemaFixLogicalType.fixLogicalTypeHelper(copySchema)
  }

  def fixLogicalType(avroSchema: String): Schema = {
    val copySchema =  new Schema.Parser().parse(avroSchema)
    new Schema.Parser().parse(SchemaFixLogicalType.fixLogicalTypeHelper(copySchema).toString())
  }


  def fixLogicalTypeHelper(schema: Schema): Schema = {
    schema.getType match {
      case INT =>
        if (schema.getProp("connect.name") != null  && schema.getLogicalType == null) {
            val connectName = schema.getProp("connect.name")
            connectName match {
              case "io.debezium.time.Date" =>
                schema.addProp("logicalType","date")
              case _ =>
            }
          }
        schema
      case STRING =>
        if (schema.getProp("connect.name") != null  && schema.getLogicalType == null) {
          val connectName = schema.getProp("connect.name")
            connectName match {
              case "io.debezium.time.ZonedTimestamp" =>
                schema.addProp("logicalType", "timestamp-zoned")
              case _ =>
            }
        }
        schema
      case BOOLEAN => schema
      case BYTES | FIXED => schema
      case DOUBLE => schema
      case FLOAT => schema
      case LONG =>
        if (schema.getProp("connect.name") != null  && schema.getLogicalType == null) {
          val connectName = schema.getProp("connect.name")
            connectName match {
              case "io.debezium.time.Timestamp" =>
                schema.addProp("logicalType", "timestamp-millis")
              case "io.debezium.time.MicroTimestamp" =>
                schema.addProp("logicalType", "timestamp-micros")
              case "io.debezium.time.ZonedTimestamp" =>
                schema.addProp("logicalType", "timestamp-zoned")
              case "io.debezium.time.MicroTime" =>
                schema.addProp("logicalType", "time-micros")
              case _ =>
            }
        }
        schema
      case ENUM => schema
      case RECORD =>
        val fields = schema.getFields
        val newFields = new util.ArrayList[Schema.Field](fields.size)
        for (i <- 0 until fields.size()) {
          val newField = new Schema.Field(fields.get(i).name(), fixLogicalTypeHelper(fields.get(i).schema()), fields.get(i).doc(),fields.get(i).defaultVal())
          newFields.add(newField)
        }
        Schema.createRecord(schema.getName, schema.getDoc, schema.getNamespace, false, newFields)
      case ARRAY => schema
      case MAP => schema
      case UNION =>
        val types = schema.getTypes
        val newSchemas = new util.ArrayList[Schema](types.size)
        for (i <- 0 until types.size()) {
          newSchemas.add(fixLogicalTypeHelper(types.get(i)))
        }
        Schema.createUnion(newSchemas)
      case NULL => schema
      case other => throw new IncompatibleSchemaException(s"Unsupported type $other")
    }
  }

}
