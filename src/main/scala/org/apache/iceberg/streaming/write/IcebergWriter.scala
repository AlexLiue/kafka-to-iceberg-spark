package org.apache.iceberg.streaming.write

import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericRecord
import org.apache.iceberg.streaming.Kafka2Iceberg.{icebergMaintenanceMaps, schemaBroadcastMaps, statusAccumulatorMaps}
import org.apache.iceberg.streaming.avro.{AvroConversionHelper, TimestampZoned, TimestampZonedFactory}
import org.apache.iceberg.streaming.config.{RunCfg, TableCfg}
import org.apache.iceberg.streaming.core.accumulator.StatusAccumulator
import org.apache.iceberg.streaming.core.broadcast.SchemaBroadcast
import org.apache.iceberg.streaming.utils.{DDLUtils, SchemaUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable


/**
 * Iceberg 数据存储对象
 */
object IcebergWriter  extends Logging {


  /**
   * Write DataFrame MERGE INTO Iceberg Table
   * @param spark  SparkSession
   * @param tableCfg TableCfg
   * @param df DataFrame to be write
   * @param curSchema Avro Schema
   */
  def writeToIceberg(spark: SparkSession,
                     tableCfg: TableCfg,
                     df: DataFrame,
                     curSchema: Schema): Unit = {
    val cfg = tableCfg.getCfgAsProperties
    val icebergTableName: String = cfg.getProperty(RunCfg.ICEBERG_TABLE_NAME)
    val primaryKey = cfg.getProperty(RunCfg.ICEBERG_TABLE_PRIMARY_KEY)
    val sourcePrefix = cfg.getProperty(RunCfg.RECORD_METADATA_SOURCE_PREFIX).trim

    DDLUtils.createTableIfNotExists(spark, tableCfg, df.schema)
    val tempTable = s"${icebergTableName.replace(".","_")}_temp"
    df.createOrReplaceTempView(tempTable)

    val writeSql =
      s"""
        |MERGE INTO $icebergTableName AS t
        |USING (SELECT * from $tempTable) AS s
        |ON ${primaryKey.split(",").map(key => s"t.${key.trim} = s.${key.trim}").mkString(" and ")}
        |WHEN MATCHED AND s.${sourcePrefix}op = 'u' THEN UPDATE SET *
        |WHEN MATCHED AND s.${sourcePrefix}op = 'd' THEN DELETE
        |WHEN NOT MATCHED THEN INSERT *
        |""".stripMargin

    spark.sql(writeSql)
  }

  /**
   * @param spark    SparkSession
   * @param rdd      读取的 Kafka 数据
   * @param tableCfg 数据处理配置信息
   * @param useCfg   用户通过 main 函数输入的配置信息
   */
  def write(spark: SparkSession,
            rdd: RDD[ConsumerRecord[GenericRecord, GenericRecord]],
            tableCfg: TableCfg,
            useCfg: Properties
           ): Unit = {

    /* 解析配置信息. */
    val props = tableCfg.getCfgAsProperties
    val icebergTableName: String = props.getProperty(RunCfg.ICEBERG_TABLE_NAME)
    val statusAcc: StatusAccumulator = statusAccumulatorMaps(icebergTableName)
    val schemaBroadcast: Broadcast[SchemaBroadcast] = schemaBroadcastMaps(icebergTableName)
    val maintenance: IcebergMaintenance = icebergMaintenanceMaps(icebergTableName)

    logInfo(s"beginning write data into $icebergTableName ..." )

    val curSchemaVersion = statusAcc.schemaVersion
    val curSchema =  schemaBroadcast.value.versionToSchemaMap(curSchemaVersion)
    val curStructType = SchemaUtils.convertSchemaToStructType(curSchema)

    val sourceIndex: Seq[Int] = SchemaUtils.getSourceFieldIndex(tableCfg, curSchema)
    val transactionIndex: Seq[Int] =  SchemaUtils.getTransactionFieldIndex(tableCfg, curSchema)
    val kafkaColumns: Seq[String] = tableCfg.getCfgAsProperties.getProperty(RunCfg.RECORD_METADATA_KAFKA_COLUMNS).split(",").map(_.trim)

    val rddRow = rdd.mapPartitions[Row](
     records => {
        LogicalTypes.register(TimestampZoned.TIMESTAMP_ZONED, new TimestampZonedFactory())
        /* Schema HashCode 计算方式与 Java Run ENV 有关, 因此需在 Executor 节点中计算 hashCode */
        /* curSchemaHashCode 用于快速对比判断当前处理记录的 Schema hashCode 是否更新，不考虑 hash 碰撞问题 */
        val curSchemaHashCode = curSchema.hashCode()
        val convertor = AvroConversionHelper.createConverterToRow(curSchema, curStructType)

        val keyedRowMap = new mutable.HashMap[GenericRecord, Row]()
        records.foreach {
          record: ConsumerRecord[GenericRecord, GenericRecord] => {
            if (record.value.getSchema.hashCode().equals(curSchemaHashCode)) {
              statusAcc.updateCurOffset(record)
              val (key, value) = convertorGenericRecordToRow(record, convertor, sourceIndex, transactionIndex, kafkaColumns)
              if (value != null) {
                keyedRowMap.put(key, value)
              }
            }
          }
        }
        keyedRowMap.values.iterator
      }
    )
    val structType = generateStructType(tableCfg, curSchema, curStructType, sourceIndex, transactionIndex,kafkaColumns)
    val df = spark.createDataFrame(rddRow, structType)
    df.show(false)
    writeToIceberg(spark, tableCfg, df, curSchema)
    logInfo(s"finished write data into $icebergTableName ..." )

    /* Maintenance Table */
   if(maintenance.checkTriggering()){
     /* 执行表维护 */
     maintenance.launchMaintenance()

     /* 更新历史执行时间并重置标识 */
     icebergMaintenanceMaps += (icebergTableName -> maintenance.updateAndResetTrigger)
   }
  }

  /**
   * 构建创建 DataFrame 所需的 StructType 数据类型定义对象
   * @return
   */
  def generateStructType(
                          tableCfg: TableCfg,
                          curSchema: Schema,
                          curStructType: StructType,
                          sourceIndex: Seq[Int],
                          transIndex: Seq[Int],
                          kafkaColumns: Seq[String]
                        ): StructType = {
    val dataColumnSize = curSchema.getField("after").schema().getTypes.asScala.filter(_.getType != Type.NULL).head.getFields.size()
    val structFieldSize = sourceIndex.length + 2 + transIndex.length + kafkaColumns.size + dataColumnSize
    val structFields  = new java.util.ArrayList[StructField](structFieldSize)
    val cfg = tableCfg.getCfgAsProperties
    val curStructFields = curStructType.fields

    /* 附加 Metadata Source 信息 */
    val sourcePrefix = cfg.getProperty(RunCfg.RECORD_METADATA_SOURCE_PREFIX).trim
    /* 原始的 Source StructField [列已重命名-附加前缀] */
    val sourceStructFields  = curStructFields.apply(curSchema.getField("source").pos()).dataType.
      asInstanceOf[StructType].fields.map(x => StructField(sourcePrefix+x.name, x.dataType, nullable = true, x.metadata))
    sourceIndex.foreach(x => structFields.add(sourceStructFields.apply(x)))

    /* 附加 Metadata  opType / debeziumTime */
    val optField = curStructFields.apply(curSchema.getField("op").pos())  /* 数据库的操作类型 */
    val tsField = curStructFields.apply(curSchema.getField("ts_ms").pos()) /* log 解析时间 */
    structFields.add(StructField(sourcePrefix+optField.name, optField.dataType, nullable = true, optField.metadata))
    structFields.add(StructField(sourcePrefix+tsField.name+"_r", tsField.dataType, nullable = true, tsField.metadata))

    /* 附加 Metadata Transaction 信息 */
    val transPrefix = cfg.getProperty(RunCfg.RECORD_METADATA_TRANSACTION_PREFIX).trim
    /* 原始的 Transaction StructField [列已重命名-附加前缀]  */
    val transStructFields: Array[StructField]  = curStructFields.apply(curSchema.getField("transaction").pos()).
      dataType.asInstanceOf[StructType].fields.map(x => StructField(transPrefix+x.name, x.dataType, nullable = true, x.metadata))
    transIndex.foreach(x => structFields.add(transStructFields.apply(x)))

    /* 附加 Kafka 相关信息: topic, partition, offset, timestamp */
    /* Kafka 域字段重命名附加前缀 */
    val kafkaPrefix = cfg.getProperty(RunCfg.RECORD_METADATA_KAFKA_PREFIX).trim
    for (column <- kafkaColumns) {
      column match {
        case "topic" =>  structFields.add(StructField(kafkaPrefix + column, StringType, nullable = true))
        case "partition" => structFields.add(StructField(kafkaPrefix + column, IntegerType, nullable = true))
        case "offset" => structFields.add(StructField(kafkaPrefix + column, LongType, nullable = true))
        case "timestamp" => structFields.add(StructField(kafkaPrefix + column, LongType, nullable = true))
        case _ => logWarning(s"Unknown kafka metadata column [$column]")
      }
    }

    /* 附加 Column Data Value [ 数值 统一使用 after 域的类型定义 ]*/
    val afterStructFields: Array[StructField]  =
      curStructFields.apply(curSchema.getField("after").pos()).
        dataType.asInstanceOf[StructType].fields.map(x => StructField(x.name.toLowerCase(), x.dataType, nullable= true, x.metadata))
    for(field <- afterStructFields){
      structFields.add(field)
    }

    StructType.apply(structFields)
  }

  /**
   * 将 GenericRecord 对象转换为 Spark SQL Row 对象 并 扁平化 Row
   *
   * @param consumerRecord   GenericRecord, 从中截取附加信息, 比如消息在 kafka 中的 topic， partition,offset,timestamp 等
   * @param convertor 将 GenericRecord 对象转换为 Spark SQL Row 对象
   * @return Row 扁平化处理的结构, Row 结构 [source, opType, debeziumTime, before/after ]
   */
  def convertorGenericRecordToRow(
                                   consumerRecord: ConsumerRecord[GenericRecord, GenericRecord],
                                   convertor: AnyRef => AnyRef,
                                   sourceIndex: Seq[Int],
                                   transactionIndex: Seq[Int],
                                   kafkaColumns: Seq[String]
                                 ): (GenericRecord , Row) = {
    val record = consumerRecord.value()
    if (record == null) {
      return null
    }
    /* 将 GenericRecord 对象转换为 Spark SQL Row 对象 */
    val row = convertor(record).asInstanceOf[Row]
    val recordSchema = record.getSchema

    /* Unwrap Spark SQL Row 对象: 扁平化处理,并附加 Metadata 信息 */
    val opType: String = record.get("op").toString
    val dataRow: Row = {
      if (opType.equals("u") || opType.equals("c") || opType.equals("r")) {
        row.get(recordSchema.getField("after").pos()).asInstanceOf[Row]
      }
      else if (opType.equals("d")) {
        row.get(recordSchema.getField("before").pos()).asInstanceOf[Row]
      }
      else {
        null
      }
    }
    if (dataRow != null) {
      val sourceSize = sourceIndex.size
      val transactionSize = transactionIndex.size

      val valueSize = sourceSize + transactionSize + 2 + kafkaColumns.size + dataRow.size
      val values = new Array[Any](valueSize)

      /* 附加 Metadata Source 信息 */
      val sourceRow: Row = row.get(recordSchema.getField("source").pos()).asInstanceOf[Row]
      for (i <- 0 until sourceSize) {
        values.update(i, sourceRow(sourceIndex(i)))
      }

      /* 附加 Metadata  opType / debeziumTime */
      val debeziumTime: Long = row.get(record.getSchema.getField("ts_ms").pos()).asInstanceOf[Long]
      values.update(sourceSize, opType)
      values.update(sourceSize  + 1, debeziumTime)

      /* 附加 Metadata Transaction 信息 */
      val transaction = row.get(record.getSchema.getField("transaction").pos())
      val tranIndexOffset = sourceSize + 2
      if(transaction != null){
        val transactionRow: Row = transaction.asInstanceOf[Row]
        for (i <- 0 until transactionSize) {
          values.update(tranIndexOffset + i, transactionRow(transactionIndex(i)))
        }
      }else{
        for (i <- 0 until transactionSize) {
          values.update(tranIndexOffset + i, null)
        }
      }

      /* 附加 Kafka 相关信息: topic, partition, offset, timestamp */
      val kafkaIndexOffset = tranIndexOffset + transactionSize
      for (i <- kafkaColumns.indices) {
        kafkaColumns(i) match {
          case "topic" => values.update(kafkaIndexOffset + i, consumerRecord.topic())
          case "partition" => values.update(kafkaIndexOffset + i, consumerRecord.partition())
          case "offset" => values.update(kafkaIndexOffset + i, consumerRecord.offset())
          case "timestamp" => values.update(kafkaIndexOffset + i, consumerRecord.timestamp())
          case _ => logWarning(s"Unknown kafka metadata [${kafkaColumns(i)}]")
        }
      }

      /* 附加 Column Data Value */
      val dataIndexOffset = kafkaIndexOffset + kafkaColumns.size
      for (i <- 0 until dataRow.size) {
        values.update(dataIndexOffset + i, dataRow(i))
      }
      /* 返回 GenericRow 对象 */
      (consumerRecord.key(), new GenericRow(values))
    }else{
      (consumerRecord.key(), null)
    }
  }

}
