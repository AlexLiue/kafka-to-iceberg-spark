package org.apache.iceberg.streaming

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.iceberg.streaming.config.{Config, JobCfgHelper, RunCfg, TableCfg}
import org.apache.iceberg.streaming.write.IcebergWriter
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext, kafka010}
import org.apache.iceberg.streaming.core.accumulator.{PartitionOffset, StatusAccumulator}
import org.apache.iceberg.streaming.core.broadcast
import org.apache.iceberg.streaming.core.broadcast.SchemaBroadcast
import org.apache.iceberg.streaming.exception.SchemaChangedException
import org.apache.iceberg.streaming.utils.{HadoopUtils, HiveUtils, SchemaUtils}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.InputDStream

import java.io.StringReader
import java.util.Properties
import scala.collection.convert.ImplicitConversions.`map AsScala`
import scala.collection.immutable.HashMap

class Kafka2Iceberg {}

/**
 * 程序运行入口
 */
object Kafka2Iceberg extends Logging{

  /* Schema 共享数据信息 （Broadcast / Accumulator). */
  var statusAccumulatorMaps: HashMap[String, StatusAccumulator] = _
  var schemaBroadcastMaps: HashMap[String, Broadcast[SchemaBroadcast]] = _

  /* 是否重启 Steaming 流(当数据 Schema 发生变动时需重启，以重复消费被上个 batch 丢弃的新 Schema Version 数据) */
  var restartStream: Boolean = _
  var stopStream: Boolean = _

  /* Spark Context */
  var spark: SparkSession = _
  var ssc: StreamingContext = _


  var confKey: String = _
  var batchDuration: Long = _
  var runEnv: String = _
  var enableDropColumn: Boolean = _


  /**
   * 主函数入口.
   * @param args Array[String]
   */
  def main(args: Array[String]): Unit = {
    /* User Input Config */
    val useCfg = Config.getInstance().loadConf(args).toProperties
    confKey = useCfg.getProperty("confKey")
    batchDuration = useCfg.getProperty("batchDuration").toInt
    runEnv = useCfg.getProperty("runEnv", RunCfg.RUN_ENV_DEFAULT) /* test or product. */
    enableDropColumn = useCfg.getProperty("enableDropColumn","true").toBoolean

    /* Load Job Config Form Config Database */
    import scala.collection.JavaConverters._
    val confAddressProps: Properties = new Properties()
    confAddressProps.load(classOf[Kafka2Iceberg].getClassLoader.getResourceAsStream("conf-address.properties"))
    val cfgArr = JobCfgHelper.getInstance.getConf(confKey, confAddressProps, runEnv).asScala.toArray

    /* 使用第一张表的配置来创建 SparkSession. */
    val headProp =  cfgArr.head.getCfgAsProperties
    val sparkConfigKeys = headProp.keySet().asScala.map(_.toString).filter(_.startsWith("spark"))
    val sparkConf = new SparkConf()
    for(key <- sparkConfigKeys){
      sparkConf.set(key, headProp.getProperty(key))
    }
    spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    /* 创建 Spark Streaming Context 对象，并指定批次处理间隔. */
    ssc = new StreamingContext(spark.sparkContext, Seconds(batchDuration))


    /* 初始化共享变量 */
    initRuntimeEnv(spark, cfgArr)
    restartStream = false  /* 当检测到 Schema 发生变化时重启, 回溯消费位点以重复处理被上次 batch 作业被丢弃的数据 */
    stopStream = false

    while(!stopStream){
      try {
        for (cfg <- cfgArr) {
          startStreamJob(cfg, useCfg)
          ssc.start()
          ssc.awaitTermination()
        }
      } catch {
        case _: SchemaChangedException =>
          restartStream(cfgArr)
        case e: Exception =>
          logError("Found unknown exception , stop job...")
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          stopStream=true
          throw e
      }
    }

    spark.stop()
  }



  def restartStream(cfgArr: Array[TableCfg]):Unit = {
    /* 停止 Spark Streaming - 立即停止,避免与 Spark Streaming 和 后续的手动提交 Kafka Offset 互相冲突 */
    logWarning("Detect Schema Version Changed, Restart SparkStream Job...")
    ssc.stop(stopSparkContext = false, stopGracefully = false)  /* 强制停止, 否则会继续执行被 Schedule 的 Batch */

    /* 由于 SparkStream 仅支持异步提交 Kafka Commit Offset, 因此需要强制同步 Commit Offset. */
    Thread.sleep(3000)  /* 暂停 3 秒等待 Kafka consumer timeout */
    for (cfg <- cfgArr) {
      val icebergTable: String = cfg.getCfgAsProperties.getProperty(RunCfg.ICEBERG_TABLE_NAME)
      val offsets = statusAccumulatorMaps(icebergTable).convertToKafkaCommitOffset()
      logInfo(s"commit kafka offset by kafka client [${offsets.mkString(",")}]")
      utils.KafkaUtils.commitSync(cfg, offsets)
    }

    /* 重新初始化共享变量 */
    logInfo("Restarted initAccumulatorAndBroadcast ...")
    initRuntimeEnv(spark, cfgArr)
    restartStream = false  /* 当检测到 Schema 发生变化时重启, 回溯消费位点以重复处理被上次 batch 作业被丢弃的数据 */
    stopStream = false

    /* 启动 Spark Streaming -  */
    ssc = new StreamingContext(spark.sparkContext, Seconds(batchDuration))
    logInfo("Restarted StreamingContext ...")
  }


  /**
   * 根据作业配置启动一个数据流作业
   * @param tableCfg TableCfg 作业运行配置
   * @param useCfg 用户输入参数
   */
  def startStreamJob(tableCfg: TableCfg, useCfg: Properties): Unit = {
    val prop = new Properties()
    prop.load(new StringReader(tableCfg.getConfValue))
    val bootstrapServers: String = prop.getProperty(RunCfg.KAFKA_BOOTSTRAP_SERVERS)
    val schemaRegistryUrl: String = prop.getProperty(RunCfg.KAFKA_SCHEMA_REGISTRY_URL)
    val groupId: String = prop.getProperty(RunCfg.KAFKA_CONSUMER_GROUP_ID)
    val topics: Array[String] = prop.getProperty(RunCfg.KAFKA_CONSUMER_TOPIC).split(",")
    val maxPollRecords: String = prop.getProperty(RunCfg.KAFKA_CONSUMER_MAX_POLL_RECORDS)
    val keyDeserializer: String = prop.getProperty(RunCfg.KAFKA_CONSUMER_KEY_DESERIALIZER)
    val valueDeserializer: String = prop.getProperty(RunCfg.KAFKA_CONSUMER_VALUE_DESERIALIZER)
    val icebergTable: String = prop.getProperty(RunCfg.ICEBERG_TABLE_NAME)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "schema.registry.url" -> schemaRegistryUrl,
      "group.id" -> groupId,
      "max.poll.records" -> maxPollRecords,
      "key.deserializer" -> keyDeserializer,
      "value.deserializer" -> valueDeserializer,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream: InputDStream[ConsumerRecord[GenericRecord, GenericRecord]] =
      kafka010.KafkaUtils.createDirectStream[GenericRecord, GenericRecord](ssc, PreferConsistent,
        Subscribe[GenericRecord, GenericRecord](topics, kafkaParams))

    stream.foreachRDD {
      rdd =>
        if (!rdd.isEmpty()) {
          logInfo("----------------------------------------------------------------------------------")
          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          val  partitionOffsets = HashMap(offsetRanges.map(
            offsetRange =>
              (s"${offsetRange.topic}:${offsetRange.partition}", new PartitionOffset(
                offsetRange.topic, offsetRange.partition, offsetRange.fromOffset,
                offsetRange.untilOffset, offsetRange.fromOffset))
          ): _*)

          /* 更新 PartitionOffset 状态（ 用于后续 untilOffset 和 curOffset 进行对比判断数据 Schema 是否存在更新）*/
          statusAccumulatorMaps(icebergTable).updatePartitionOffsets(partitionOffsets)

          logInfo(s"before write accumulator value [${statusAccumulatorMaps(icebergTable).toString}]")
          logInfo(s"current offset ranges: [${offsetRanges.mkString(",")}]")
          logInfo(s"current partition offsets: [${partitionOffsets.mkString(",")}]")

          IcebergWriter.write(spark, rdd, tableCfg, useCfg)

          /* 提交 Kafka Offset */
          val commitOffsetRangers = statusAccumulatorMaps(icebergTable).getCurrentOffsetRangers
          logInfo(s"commit offset rangers: [${commitOffsetRangers.mkString(",")}]")
          stream.asInstanceOf[CanCommitOffsets].commitAsync(commitOffsetRangers, new OffsetCommitCallback() {
            def onComplete(m: java.util.Map[TopicPartition, OffsetAndMetadata], e: Exception): Unit = {
              m.foreach(f => {
                if (null != e) {
                  logError("Failed to commit:" + f._1 + "," + f._2)
                  logError("Error while commitAsync. Retry again"+e.toString)
                  try{
                    stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
                  }catch {
                    case re: Exception =>
                      throw re
                  }
                } else {
                  logInfo("kafka offset commit by spark streaming:" + f._1 + "," + f._2)
                }
              })
            }
          })
          logInfo("----------------------------------------------------------------------------------\n")
          /* 数据处理结束后，进行 Schema 更新检测： 如果所有分区的 Schema 发生改变，重启 Stream 流作业，以更新 Schema 版本 */
          val afterStatusAcc = statusAccumulatorMaps(icebergTable)
          logInfo(s"after write accumulator value [${afterStatusAcc.toString}]")

          /* 如果所有数据的 schema 均已发生变动, 抛出 Schema 版本更新异常以终止结束 Stream 作业, 然后捕捉异常并重启... */
          if (isAllPartitionSchemaChanged(tableCfg)) {
            throw new SchemaChangedException("detect schema version changed, need restart spark streaming job...")
          }else{
            logInfo("schema changed: false ... ")
          }
        } else {
          logInfo(s"table streaming rdd is empty ...")
        }
    }
  }

  /**
   * 根据 当前的  StatusAccumulator 判断 schema 是否需要更新
   * @param tableCfg TableCfg
   * @return
   */
  def isAllPartitionSchemaChanged(tableCfg: TableCfg):Boolean = {
    val props = tableCfg.getCfgAsProperties
    val icebergTable: String = props.getProperty(RunCfg.ICEBERG_TABLE_NAME)
    val statusAcc = statusAccumulatorMaps(icebergTable)
    val po =  statusAcc.partitionOffsets.values

    /* 如果所有分区 curOffset = untilOffset， 即所有分区 schema 均未改变, 均没有丢弃数据 */
    if(po.size == po.count(p => p.curOffset == p.untilOffset)){
      return false
    }

    /*  如果 curOffset < untilOffset, 即数处理过程丢弃了部分数据,则认为该分区的 Schema 发生了变动 */
    /* 如果所有分区的 curOffset < untilOffset, 即所有的 partition 的 schema 都发生了更新 */
    if(po.size == po.count(p => p.curOffset < p.untilOffset)){
      return true
    }

    /* 否则根据 curOffset, 从 Kafka 中读取所有分区的后一条数据( curOffset + 1), 并获取所有分区数据的最小 schema version, 然后与当前 _schemaVersion 对比 */
    val nextBatchMinSchemaVersion = SchemaUtils.getNextBatchMinSchemaVersion(tableCfg)
    /* 如果下一批次数据最小 schema 版本 大于当前批次的 schema 版本, 则认定所有旧 schema 的版本数据均已处理完成, 即所有分区的 schema 均已发生改变 */
    if(nextBatchMinSchemaVersion > statusAcc.schemaVersion){
      true
    }else{
      false
    }
  }


  /**
   * 初始化共享变量
   */
  def initRuntimeEnv(spark: SparkSession, cfgArr: Array[TableCfg]): Unit = {
    /* 初始化 Schema 共享数据信息 （Broadcast / Accumulator） */
    statusAccumulatorMaps= HashMap[String, StatusAccumulator]()
    schemaBroadcastMaps= HashMap[String, Broadcast[SchemaBroadcast]]()
    for (cfg <- cfgArr) {
      /* 配置解析. */
      val props =  cfg.getCfgAsProperties
      logInfo(s"Table Configure [${props.mkString(", ")}]")
      val icebergTableName = props.getProperty(RunCfg.ICEBERG_TABLE_NAME).trim  /* Iceberg 表名 */

      /* 初始化 StatusAccumulator 信息, 通过 Kafka Committed Offset 获取历史的 Schema Version 信息 */
      val schemaVersion = SchemaUtils.getNextBatchMinSchemaVersion(cfg) /* Avro Version. */
      val statusAcc = StatusAccumulator.registerInstance(spark.sparkContext, icebergTableName)
      statusAcc.setSchemaVersion(schemaVersion)
      statusAcc.initPartitionOffset(cfg)
      statusAccumulatorMaps += (icebergTableName -> statusAcc)

      /* 初始化 SchemaBroadcaster 信息. */
      val schemaToVersionMap: HashMap[Schema, Integer] = SchemaUtils.getSchemaToVersionMap(cfg)
      val versionToSchemaMap: HashMap[Integer, Schema] = SchemaUtils.getVersionToSchemaMap(cfg)
      val schemaBroadcast = broadcast.SchemaBroadcast(schemaToVersionMap, versionToSchemaMap)
      schemaBroadcastMaps += (icebergTableName -> spark.sparkContext.broadcast(schemaBroadcast))
    }

    logInfo(s"After Init StatusAccumulator [${statusAccumulatorMaps.mkString("\nMap(\n    ",",\n    ",")")}]")
    logInfo(s"After Init SchemaBroadcast   [${schemaBroadcastMaps.map(x => (x._1, x._2.value)).mkString("\nMap(\n    ",",\n    ",")")}]")

    /* 检测 更新 iceberg 表结构 */
    for (cfg <- cfgArr) {
      val prop = cfg.getCfgAsProperties
      val icebergTableName = prop.getProperty(RunCfg.ICEBERG_TABLE_NAME)
      val statusAcc = statusAccumulatorMaps(icebergTableName)
      val schemaBroadcast = schemaBroadcastMaps(icebergTableName)
      if(statusAcc.schemaVersion > 1){
        val schema = schemaBroadcast.value.versionToSchemaMap(statusAcc.schemaVersion)
        val dataFieldSchema = schema.getField("after").schema() /* 取 after 数值域的 schema */
        val schemaUpdated = HadoopUtils.checkAndAlterHadoopTableSchema(spark,
          icebergTableName, dataFieldSchema, enableDropColumn)

        /* 如果 Hadoop Catalog Table 表结构发生了变动重新创建 Hive 外表 */
        if(schemaUpdated){
          val props = cfg.getCfgAsProperties
          val hiveJdbcUrl = props.getProperty(RunCfg.HIVE_JDBC_URL)
          val hiveJdbcUser =  props.getProperty(RunCfg.HIVE_JDBC_USER)
          val hiveJdbcPassword =  props.getProperty(RunCfg.HIVE_JDBC_PASSWORD)
          val hiveExtendJars =  props.getProperty(RunCfg.HIVE_EXTEND_JARS)
          val hadoopWarehouse =  props.getProperty(RunCfg.SPARK_SQL_CATALOG_HADOOP_WAREHOUSE)
          HiveUtils.createOrReplaceHiveTable(hiveJdbcUrl, hiveJdbcUser, hiveJdbcPassword,
            hiveExtendJars,icebergTableName,hadoopWarehouse)
        }
      }
    }
  }

}
