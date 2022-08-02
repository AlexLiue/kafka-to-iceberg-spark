package org.apache.iceberg.streaming.utils

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.streaming.Kafka2Iceberg.statusAccumulatorMaps
import org.apache.iceberg.streaming.avro.{SchemaConverters, SchemaFixLogicalType}
import org.apache.iceberg.streaming.config.{RunCfg, TableCfg}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.kafka010.OffsetRange

import java.util
import java.util.{Collections, Properties}
import scala.collection.convert.ImplicitConversions.`map AsScala`
import scala.collection.immutable.HashMap

/**
 * Schema 工具类
 */
object SchemaUtils extends Logging{

  /**
   * 获取 Schema Version, 该 Version 被用于作业初始化时的 Innit Version
   *  - 如果 Commit Offset 为空 则取值 beginningOffsets
   *  - 如果 存在 多个 topic ,则所有的 Topic 的 Schema 应当保持相同迭代版本
   *
   * @return Kafka Committed Offset
   */

  def getNextBatchMinSchemaVersion(tableCfg: TableCfg): Int = {
    val props = tableCfg.getCfgAsProperties
    val icebergTable: String = props.getProperty(RunCfg.ICEBERG_TABLE_NAME)
    val bootstrapServers = props.getProperty(RunCfg.KAFKA_BOOTSTRAP_SERVERS)
    val groupId = props.getProperty(RunCfg.KAFKA_CONSUMER_GROUP_ID)
    val topics = props.getProperty(RunCfg.KAFKA_CONSUMER_TOPIC).split(",")
    val keyDeserializer = props.getProperty(RunCfg.KAFKA_CONSUMER_KEY_DESERIALIZER)
    val valueDeserializer = props.getProperty(RunCfg.KAFKA_CONSUMER_VALUE_DESERIALIZER)
    val schemaRegistryUrl = props.getProperty(RunCfg.KAFKA_SCHEMA_REGISTRY_URL)

    if(statusAccumulatorMaps.contains(icebergTable)){
      val statusAcc = statusAccumulatorMaps(icebergTable)
      val cashedCurrentOffsetRanges: Array[OffsetRange] = statusAcc.getCurrentOffsetRangers
      logInfo(s"Get next batch min schema version by StatusAccumulator cached current offset [${cashedCurrentOffsetRanges.mkString(", ")}]")
      getNextBatchMinSchemaVersion(
        cashedCurrentOffsetRanges,
        bootstrapServers, groupId, topics, keyDeserializer, valueDeserializer, schemaRegistryUrl)
    }else{
      logInfo(s"Get next batch min schema version by kafka commit offset, because StatusAccumulator status is null")
      getNextBatchMinSchemaVersion(
        null,
        bootstrapServers, groupId, topics, keyDeserializer, valueDeserializer, schemaRegistryUrl)
    }
  }

  /**
   * 获取 Schema Version, 该 Version 被用于作业初始化时的 Innit Version, 即下一个 batch 数据的最小 schema 版本值
   *  - 如果 Commit Offset 为空 则取值 beginningOffsets
   *  - 如果 存在 多个 topic ,则所有的 Topic 的 Schema 应当保持相同迭代版本
   *
   * @return Kafka Committed Offset
   */
  def getNextBatchMinSchemaVersion(cashedCurrentOffsetRanges: Array[OffsetRange],
                                   bootstrapServers: String,
                                   groupId: String,
                                   topics: Array[String],
                                   keyDeserializer: String,
                                   valueDeserializer: String,
                                   schemaRegistryUrl: String,
                                  ): Int = {
    /* 参数组装 */
    val kafkaProperties: Properties = new Properties
    kafkaProperties.setProperty("bootstrap.servers", bootstrapServers)
    kafkaProperties.setProperty("group.id", groupId)
    kafkaProperties.setProperty("key.deserializer", keyDeserializer)
    kafkaProperties.setProperty("value.deserializer", valueDeserializer)
    kafkaProperties.setProperty("schema.registry.url", schemaRegistryUrl)
    kafkaProperties.setProperty("auto.offset.reset", "earliest")
    kafkaProperties.setProperty("enable.auto.commit", "false")

    /* 创建 Kafka Consumer 对象 */
    val consumer: Consumer[GenericRecord, GenericRecord] = new KafkaConsumer[GenericRecord, GenericRecord](kafkaProperties)
    /* 获取  committedOffsets 和 beginningOffsets */
    val topicPartitions: util.Set[TopicPartition] = new util.HashSet[TopicPartition]
    for (topic <- topics) {
      val partitionInfos: util.List[PartitionInfo] = consumer.partitionsFor(topic)
      for (i  <- 0 until  partitionInfos.size()) {
        val partition: Int = partitionInfos.get(i).partition()
        val topicPartition: TopicPartition = new TopicPartition(topic, partition)
        topicPartitions.add(topicPartition)
      }
    }

    val beginningOffsets: util.Map[TopicPartition, java.lang.Long] = consumer.beginningOffsets(topicPartitions)
    val endOffsets: util.Map[TopicPartition, java.lang.Long] = consumer.endOffsets(topicPartitions)
    val committedOffsets: util.Map[TopicPartition,OffsetAndMetadata] = consumer.committed(topicPartitions)

    val client: SchemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100)

    /* Current Offset Ranges: 如果存在缓存则取缓存值，否则从 committedOffsets 取值 */
    val currentOffsetRanges: Array[OffsetRange] = if(cashedCurrentOffsetRanges != null)
    {
      cashedCurrentOffsetRanges
    }else{
      committedOffsets.map(t =>OffsetRange.apply(
        t._1.topic(),
        t._1.partition(),
        beginningOffsets.get(t._1),
        if(t._2 == null) { 0 } else { t._2.offset() }
      )).toArray
    }

    /* 所有 partition 的 schema version  */
    var schemaVersions : HashMap[TopicPartition, Int] = new HashMap[TopicPartition, Int]()

    for (currentOffsetRange <- currentOffsetRanges) {
      /* 获取 TopicPartition 和 OffsetAndMetadata */
      val topicPartition: TopicPartition = currentOffsetRange.topicPartition()
      val currentOffset: Long = currentOffsetRange.untilOffset

      /* 读取 Schema 的 fromOffset 先初始化为 beginningOffset, */
      var fromOffset: Long = beginningOffsets.get(topicPartition)
      if (currentOffset > beginningOffsets.get(topicPartition) && currentOffset < endOffsets.get(topicPartition)) {
        /* 如果 currentOffset < endOffset 则更新为 currentOffset, 读取 currentOffset offset 的后一条记录的 Schema */
        fromOffset = currentOffset
      }
      else if (currentOffset > beginningOffsets.get(topicPartition) && currentOffset == endOffsets.get(topicPartition)) {
        /*如果 currentOffset = endOffset 则更新为 currentOffset -1 ,  读取 currentOffset offset 的所在记录的 Schema */
        fromOffset = currentOffset - 1
      }
      /* 开始 seek 数据 */
      consumer.assign(Collections.singletonList(topicPartition))
      consumer.seek(topicPartition, fromOffset)

      logInfo(s"load partition [${topicPartition.toString}] schema info from offset [$fromOffset]")  /* schema 值的 offset*/

      /* 如果 1 * 5 秒内没有读取到任何数据, 则认定为该 Kafka Partition 队列为空, 结束读取等待. */
      var loopTimes: Int = 5
      var loopFlag: Boolean = true
      while ( loopFlag && loopTimes >0) {
        val records: ConsumerRecords[GenericRecord, GenericRecord] = consumer.poll(java.time.Duration.ofSeconds(1))
        if (!records.isEmpty) {
          val record: ConsumerRecord[GenericRecord, GenericRecord] = records.iterator.next
          schemaVersions += (new TopicPartition(record.topic(), record.partition()) ->
            client.getVersion(String.format("%s-value", topicPartition.topic), new AvroSchema(record.value.getSchema)))
          loopFlag = false
        }
        loopTimes = loopTimes - 1
      }
      consumer.unsubscribe() /* clean */

    }
    consumer.close()
    logInfo(s"Kafka partition schema version [${schemaVersions.mkString(",")}]")

    /* 如果所有分区的 当前 offset 均等于 kafka begin offset, 即刚开始消费, 取所有分区 schema version 的最小值 */
    if(currentOffsetRanges.count(p => {p.untilOffset == beginningOffsets.get(p.topicPartition())}) == currentOffsetRanges.length){
      logInfo("All partition current offset equal with begging offset, set next bach schema version to min version ")
      return schemaVersions.values.min
    }
    /* 如果所有分区的 当前 offset 均等于 kafka end offset, 即所有分区数据消费完毕, 取所有分区 schema version 的最大值 */
    if(currentOffsetRanges.count(p => {p.untilOffset == endOffsets.get(p.topicPartition())}) == currentOffsetRanges.length){
      logInfo("All partition current offset equal with end offset, set next bach schema version to max version")
      return schemaVersions.values.max
    }

    /* 否则在所有未消费完毕的 partition 中取最小值 */
    val unfinishedPartitions = currentOffsetRanges.filter(p => {p.untilOffset < endOffsets.get(p.topicPartition())}).map(_.topicPartition())
    val unfinishedSchemaVersions = schemaVersions.filter(p => unfinishedPartitions.contains(p._1))
    logInfo(s"Set next bach schema version to min schema version of unfinished partition [${unfinishedSchemaVersions.mkString(", ")}] ")
    unfinishedSchemaVersions.values.min
  }

  /**
   * Get Schema Registry Client Instance.
   *
   * @param tableCfg TableCfg
   * @return SchemaRegistryClient
   */
  def getSchemaRegistryClientInstance(tableCfg: TableCfg): SchemaRegistryClient = {
    val schemaRegistryUrl = tableCfg.getCfgAsProperties.getProperty(RunCfg.KAFKA_SCHEMA_REGISTRY_URL)
    getSchemaRegistryClientInstance(schemaRegistryUrl, AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT)
  }


  /**
   * Get Schema Registry Client Instance.
   *
   * @return SchemaRegistryClient
   */
  def getSchemaRegistryClientInstance(schemaRegistryUrl: String, cacheCapacity: Int): SchemaRegistryClient = {
    new CachedSchemaRegistryClient(schemaRegistryUrl, cacheCapacity)
  }

  /**
   * Get Schema Registry Client Instance.
   *
   * @return SchemaRegistryClient
   */
  def getSchemaRegistryClientInstance(schemaRegistryUrl: String): SchemaRegistryClient = {
    new CachedSchemaRegistryClient(schemaRegistryUrl, AbstractKafkaSchemaSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT)
  }


  /**
   * 从 Schema Registry Server 中加载最小的 Schema Version
   *
   * @param tableCfg TableCfg
   * @return Schema Version
   */
  def getMinSchemaVersion(tableCfg: TableCfg): Int = {
    val topic = tableCfg.getCfgAsProperties.getProperty(RunCfg.KAFKA_CONSUMER_TOPIC)
    val client: SchemaRegistryClient = getSchemaRegistryClientInstance(tableCfg)
    val versions: java.util.List[Integer] = client.getAllVersions(topic + "-value")
    versions.get(0)
  }

  /**
   * 从 Schema Registry Server 中加载 Schema -> Schema Version 的映射关系.
   *
   * @param tableCfg TableCfg
   * @return HashMap[String, Integer]
   */
  def getSchemaToVersionMap(tableCfg: TableCfg): HashMap[Schema, Integer] = {
    val topic = tableCfg.getCfgAsProperties.getProperty(RunCfg.KAFKA_CONSUMER_TOPIC)
    val client: SchemaRegistryClient = getSchemaRegistryClientInstance(tableCfg)
    val versions: java.util.List[Integer] = getSchemaVersionList(tableCfg)
    var schemaToVersion: HashMap[Schema, Integer] = HashMap()
    for (i <- 0 until versions.size()) {
      val schema = client.getByVersion(topic + "-value", versions.get(i), false).getSchema
      schemaToVersion += (new Schema.Parser().parse(schema) -> versions.get(i))
    }
    schemaToVersion
  }

  /**
   * 从 Schema Registry Server 中加载 Schema Version -> Schema 的映射关系.
   *
   * @param tableCfg TableCfg
   * @return
   */
  def getVersionToSchemaMap(tableCfg: TableCfg): HashMap[Integer, Schema] = {
    val topic = tableCfg.getCfgAsProperties.getProperty(RunCfg.KAFKA_CONSUMER_TOPIC)
    val client: SchemaRegistryClient = getSchemaRegistryClientInstance(tableCfg)
    val versions: java.util.List[Integer] = getSchemaVersionList(tableCfg)
    var versionToSchema: HashMap[Integer, Schema] = HashMap()
    for (i <- 0 until versions.size()) {
      val schema = client.getByVersion(topic + "-value", versions.get(i), false).getSchema
      versionToSchema += (versions.get(i) -> new Schema.Parser().parse(schema))
    }
    versionToSchema
  }

  /**
   * 从 Schema Registry Server 中加载 所有的 Schema Version
   *
   * @param tableCfg TableCfg
   * @return Schema Version List
   */
  def getSchemaVersionList(tableCfg: TableCfg): java.util.List[Integer] = {
    val topic = tableCfg.getCfgAsProperties.getProperty(RunCfg.KAFKA_CONSUMER_TOPIC)
    val client: SchemaRegistryClient = getSchemaRegistryClientInstance(tableCfg)
    client.getAllVersions(topic + "-value")
  }


  def getSchemaByVersion(tableCfg: TableCfg, schemaVersion: Int): String = {
    val topic = tableCfg.getCfgAsProperties.getProperty(RunCfg.KAFKA_CONSUMER_TOPIC)
    val client: SchemaRegistryClient = getSchemaRegistryClientInstance(tableCfg)
    client.getByVersion(topic + "-value", schemaVersion, false).getSchema

  }


  /**
   * 获取 初始化 Merged Schema 信息
   * 1、如果配置中未找到 avro.schema.merged.string 配置, 则认定为第一次执行, 从 Schema Registry Server 中读取最小的 Schema 版本信息
   * 2、如果配置中存在 avro.schema.merged.string, 则解析配置并返回
   *
   * @param tableCfg TableCfg
   * @return Init Avro Schema
   */
  def getInitMergedSchemaStr(tableCfg: TableCfg): String = {
    val cfgSchemaStr = tableCfg.getCfgAsProperties.getProperty(RunCfg.AVRO_SCHEMA_MERGED_STRING)
    val schemaStr =
      if (cfgSchemaStr == null || cfgSchemaStr.trim.equals("")) {
        val initSchemaVersion = getMinSchemaVersion(tableCfg)
        getSchemaByVersion(tableCfg, initSchemaVersion)
      } else {
        cfgSchemaStr
      }
    schemaStr
  }


  /**
   * 获取 初始化 Schema 信息 - 并根据 SchemaConverters  转换为Spark StructType 类型
   *
   * @param tableCfg TableCfg
   * @return StructType
   */
  @deprecated("instead by convertSchemaToStructType")
  def getInitMergedStructV1(tableCfg: TableCfg): StructType = {
    val schema = getInitMergedSchemaStr(tableCfg)
    val targetSchema = SchemaFixLogicalType.fixLogicalType(schema)
    SchemaConverters.toSqlType(targetSchema).dataType.asInstanceOf[StructType]
  }

  /**
   * 获取 初始化 Schema 信息 -
   * 由于现有接口不支持从 Avro Schema 直接转换为 Spark StructType 类型
   * 首先并根据 [[AvroSchemaUtil]] 将 Avro Schema 间接转换为 iceberg schema
   * 最后使用 [[org.apache.iceberg.spark.SparkSchemaUtil]] 将  iceberg schema 转换为 Spark StructType 类型
   *
   * @param tableCfg TableCfg
   * @return StructType
   */
  @deprecated("instead by convertSchemaToStructType ")
  def getInitMergedStructV2(tableCfg: TableCfg): StructType = {
    val schema = getInitMergedSchemaStr(tableCfg)
    convertSchemaToStructType(schema)
  }

  /**
   * 由于现有接口不支持从 Avro Schema 直接转换为 Spark StructType 类型
   * 首先并根据 [[AvroSchemaUtil]] 将 Avro Schema 间接转换为 iceberg schema
   * 最后使用 [[org.apache.iceberg.spark.SparkSchemaUtil]] 将  iceberg schema 转换为 Spark StructType 类型
   *
   * @param schema Stranded Avro Schema
   * @return StructType
   */
  def convertSchemaToStructType(schema: Schema): StructType = {
    org.apache.iceberg.spark.SparkSchemaUtil.convert(AvroSchemaUtil.toIceberg(schema))
  }

  /**
   * 由于现有接口不支持从 Avro Schema 直接转换为 Spark StructType 类型
   * 首先并根据 [[AvroSchemaUtil]] 将 Avro Schema 间接转换为 iceberg schema
   * 最后使用 [[org.apache.iceberg.spark.SparkSchemaUtil]] 将  iceberg schema 转换为 Spark StructType 类型
   *
   * @param schemaStr Stranded Avro Schema Json String
   * @return StructType
   */
  def convertSchemaToStructType(schemaStr: String): StructType = {
    convertSchemaToStructType(new Schema.Parser().parse(schemaStr))
  }


  /**
   * 获取 Metadata 的 Source 域中需要解析的字段的 index
   *
   * @param tableCfg TableCfg
   * @param schema   Original Avro Schema
   * @return
   */
  def getSourceFieldIndex(tableCfg: TableCfg, schema: Schema): Seq[Int] = {
    val sourceFields = getMetadataSourceFields(tableCfg)
    val sourceSchema = schema.getField("source").schema()
    sourceFields.map(x => sourceSchema.getField(x).pos())
  }

  /**
   * 获取 Metadata 的 Source 域中需要解析的字段的 ts_ms 字段的 index （作为 Partition 计算字段）
   *
   * @param tableCfg TableCfg
   * @param schema   Original Avro Schema
   * @return
   */
  def getSourceTsMsFieldIndex(tableCfg: TableCfg, schema: Schema): Int = {
    val sourceFields = getMetadataSourceFields(tableCfg)
    val sourceSchema = schema.getField("source").schema()
    sourceSchema.getField("ts_ms").pos()
  }

  /**
   * 从配置文件获取 Metadata 的 Source 域中需要解析的字段名
   *
   * @param tableCfg TableCfg
   * @return
   */
  def getMetadataSourceFields(tableCfg: TableCfg): Array[String] = {
    tableCfg.getCfgAsProperties.getProperty(RunCfg.RECORD_METADATA_SOURCE_COLUMNS).split(",").map(_.trim)
  }


  /**
   * 获取 Transaction 域中需要解析的字段的 index
   *
   * @param tableCfg TableCfg
   * @param schema   Avro Schema
   * @return Transaction Field index
   */
  def getTransactionFieldIndex(tableCfg: TableCfg, schema: Schema): Seq[Int] = {
    val metadataTransCols = tableCfg.getCfgAsProperties.getProperty(RunCfg.RECORD_METADATA_TRANSACTION_COLUMNS).split(",").map(_.trim)
    val transactionSchema = schema.getField("transaction").schema()
    val index = transactionSchema.getIndexNamed("io.confluent.connect.avro.ConnectDefault")
    val connectTransSchema = transactionSchema.getTypes.get(index)
    metadataTransCols.map(x => connectTransSchema.getField(x).pos())
  }

  /**
   * 从配置文件获取 Transaction 域中需要解析的字段名
   *
   * @param tableCfg TableCfg
   * @return Transaction Field Names
   */
  def getTransactionField(tableCfg: TableCfg): Array[String] = {
    tableCfg.getCfgAsProperties.getProperty(RunCfg.RECORD_METADATA_TRANSACTION_COLUMNS).split(",").map(_.trim)
  }


  /**
   * 将当前版本的 Schema 信息合并更新入历史的 His Merged Schema 中
   *
   * @param hisMergedSchemaStr 历史所有 Schema 版本合并信息
   * @param curAvroSchemaStr   前版本的 Schema 信息
   * @return
   *
   */
  @deprecated
  def getUpdateMergedSchemaStr(hisMergedSchemaStr: String, curAvroSchemaStr: String): String = {
    /* 历史所有 Schema 版本合并信息 */
    val hisMergedSchema = new Schema.Parser().parse(hisMergedSchemaStr)
    val hisMergedFields: util.List[Schema.Field] = hisMergedSchema.getFields

    /* 前版本的 Schema 信息 */
    val curAvroSchema = new Schema.Parser().parse(curAvroSchemaStr)
    val curAvroFields: util.List[Schema.Field] = curAvroSchema.getFields

    /* 添加当前版本 Schema 中新增的 Schema.Field 列到 Merged Schema 版本信息中 */
    for (i <- 0 until curAvroFields.size()) {
      val field = curAvroFields.get(i)
      if (!hisMergedFields.contains(field.schema())) {
        hisMergedFields.add(field)
      }
    }

    /* 返回最新的 Schema 合并结果 */
    val newMergedSchema = Schema.createRecord(hisMergedFields)
    newMergedSchema.toString()
  }

}
