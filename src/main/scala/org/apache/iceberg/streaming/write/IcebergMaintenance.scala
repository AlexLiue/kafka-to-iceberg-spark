package org.apache.iceberg.streaming.write


import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.iceberg.streaming.config.RunCfg
import org.apache.iceberg.streaming.write.IcebergMaintenance.{dateFormat, dayFormat, timeFormat}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date, Properties}


/**
 * Iceberg 表维护（清理历史快照、合并小文件等）
 * 考虑到表维护是一个比较消耗性能的操作，因此应选择在数据量比较少的时候进行维护，以降低数据堆积
 *
 *  如果 （ 当前系统时间 > triggeringTime ) 且 ((当前系统时间 - lastExecutionTime) >  executeInterval) 则执行表维护操作
 *
 * @param triggeringTime   执行  Iceberg 表维护操作的时间（24小格式: 如 21:00:00  即维护操作限制为晚21点整开始执行 ）
 * @param triggeringInterval  执行  Iceberg 表维护操作的时间间隔，单位秒 （格式: 86400000 (即 24小时)）
 * @param lastExecuteDate  执行  Iceberg 表维护操作的历史时间，格式日期: 2021-12-23 21:00:00
 *
 */
case class IcebergMaintenance (
                                spark: SparkSession,
                                enabled: Boolean,
                                triggeringTime: String,
                                triggeringInterval: Int,
                                props: Properties,
                                lastExecuteDate: String
                              )extends Logging{

  /**
   * 更新上次执行时间
   * @return
   */
  def updateAndResetTrigger(): IcebergMaintenance = {
    IcebergMaintenance(spark, enabled, triggeringTime, triggeringInterval, props, dateFormat.format(System.currentTimeMillis()))
  }

  /**
   * 启动执行表维护 - 清理快照 / 压缩小文件 / 重写 Manifests
   */
  def launchMaintenance(): Unit = {

    expireSnapshots()

    compactDataFiles()

    rewriteManifests()
  }


  /**
   * 快照清理
   */
  def expireSnapshots(): Unit = {
    val tsToExpireCfg =  props.getProperty(RunCfg.ICEBERG_MAINTENANCE_SNAPSHOT_EXPIRE_TIME)
    if(tsToExpireCfg != null){
      val tsToExpire =  System.currentTimeMillis() - tsToExpireCfg.toLong

      val icebergTableName: String = props.getProperty(RunCfg.ICEBERG_TABLE_NAME)
      val tableItems = icebergTableName.split("\\.", 3)
      val namespace = tableItems(1)
      val tableName = tableItems(2)

      /* 创建 HadoopCatalog 对象 */
      val hadoopCatalog = new HadoopCatalog()
      hadoopCatalog.setConf(spark.sparkContext.hadoopConfiguration); // Configure using Spark's Hadoop configuration
      val properties = new util.HashMap[String, String]()
      properties.put("warehouse", spark.conf.get("spark.sql.catalog.hadoop.warehouse"))
      hadoopCatalog.initialize("hadoop", properties)

      /* load table */
      val tableIdentifier = TableIdentifier.of(namespace, tableName)
      val table = hadoopCatalog.loadTable(tableIdentifier)

      logInfo(s"Iceberg maintenance execute expire snapshots with expire time [${dateFormat.format(new Date(tsToExpire))}]")
      SparkActions.
        get().
        expireSnapshots(table).
        expireOlderThan(tsToExpire).
        execute()
      hadoopCatalog.close()
      logInfo(s"Iceberg maintenance execute expire snapshots finished ")
    }
  }

  /**
   * Compact data files
   * 默认以 _src_ts_ms 数据变更时间字段，以天为单位 进行压缩选择
   *
   */
  def compactDataFiles(): Unit = {
    val icebergTableName: String = props.getProperty(RunCfg.ICEBERG_TABLE_NAME)
    val filterColumn = props.getProperty(RunCfg.ICEBERG_MAINTENANCE_COMPACT_FILTER_COLUMN)
    val dayOffset = props.getProperty(RunCfg.ICEBERG_MAINTENANCE_COMPACT_DAY_OFFSET).toInt
    val targetFileSize =  props.getProperty(RunCfg.ICEBERG_MAINTENANCE_COMPACT_FILE_SIZE_BYTES)

    val tableItems = icebergTableName.split("\\.", 3)
    val namespace = tableItems(1)
    val tableName = tableItems(2)

    /* 创建 HadoopCatalog 对象 */
    val hadoopCatalog = new HadoopCatalog()
    hadoopCatalog.setConf(spark.sparkContext.hadoopConfiguration); // Configure using Spark's Hadoop configuration
    val properties = new util.HashMap[String, String]()
    properties.put("warehouse", spark.conf.get("spark.sql.catalog.hadoop.warehouse"))
    hadoopCatalog.initialize("hadoop", properties)

    /* load table */
    val tableIdentifier = TableIdentifier.of(namespace, tableName)
    val table = hadoopCatalog.loadTable(tableIdentifier)

    /* Init lastCalendar to  lastExecuteDate + dayOffset */
    val lastDate = dateFormat.parse(lastExecuteDate)
    val lastCalendar = Calendar.getInstance
    lastCalendar.setTime(lastDate)
    lastCalendar.add(Calendar.DAY_OF_YEAR, dayOffset)

    /* filter lower bound */
    val lowerStr = s"${dayFormat.format(lastCalendar.getTime)} 00:00:00"
    val lower = dateFormat.parse(lowerStr).getTime

    /* filter upper bound */
    lastCalendar.add(Calendar.MILLISECOND, triggeringInterval)
    val upperStr = s"${dayFormat.format(lastCalendar.getTime)} 00:00:00"
    val upper = dateFormat.parse(s"${dayFormat.format(lastCalendar.getTime)} 00:00:00").getTime
    logInfo(s"Iceberg maintenance execute compact data files, with data column [filterColumn] from [$lowerStr] to [$upperStr], target file size [$targetFileSize]")
    SparkActions
      .get()
      .rewriteDataFiles(table)
      .filter(Expressions.and(
        Expressions.greaterThanOrEqual(filterColumn, lower),
        Expressions.lessThan(filterColumn, upper)))
      .option("target-file-size-bytes", targetFileSize)
      .execute()
    hadoopCatalog.close()
    logInfo(s"Iceberg maintenance execute compact data files finished ")
  }


  /**
   * 重写 Manifests 文件
   */
  def rewriteManifests():Unit = {
    val icebergTableName: String = props.getProperty(RunCfg.ICEBERG_TABLE_NAME)
    val manifestsFileLength = props.getProperty(RunCfg.ICEBERG_MAINTENANCE_MANIFESTS_FILE_LENGTH).toInt

    val tableItems = icebergTableName.split("\\.", 3)
    val namespace = tableItems(1)
    val tableName = tableItems(2)

    /* 创建 HadoopCatalog 对象 */
    val hadoopCatalog = new HadoopCatalog()
    hadoopCatalog.setConf(spark.sparkContext.hadoopConfiguration); // Configure using Spark's Hadoop configuration
    val properties = new util.HashMap[String, String]()
    properties.put("warehouse", spark.conf.get("spark.sql.catalog.hadoop.warehouse"))
    hadoopCatalog.initialize("hadoop", properties)

    /* load table */
    val tableIdentifier = TableIdentifier.of(namespace, tableName)
    val table = hadoopCatalog.loadTable(tableIdentifier)

    logInfo(s"Iceberg maintenance execute rewrite manifests to max length [$manifestsFileLength]")
    SparkActions
      .get()
      .rewriteManifests(table).rewriteIf(x => x.length() > manifestsFileLength)
      .execute()
    hadoopCatalog.close()
    logInfo(s"Iceberg maintenance execute  rewrite manifests finished ")
  }


  /**
   * 获取下次的 triggering time , 最多每天执行触发一次
   * executeInterval 需大于或等于 1 天 即 大于 86400000
   *  @return
   */
  def getNextTriggerCalendar: Calendar = {
    val lastDate = dateFormat.parse(lastExecuteDate)
    val lastCalendar = Calendar.getInstance
    lastCalendar.setTime(lastDate)

    val nextCalendar = Calendar.getInstance
    nextCalendar.setTime(lastDate)
    nextCalendar.add(Calendar.MILLISECOND, triggeringInterval)  /* 上次执行时间 +  执行间隔 = 下次执行时间 */

    val beginTriggeringTimeCalendar =  Calendar.getInstance
    beginTriggeringTimeCalendar.setTime(timeFormat.parse(triggeringTime))
    val beginTriggeringSecondOfDay = getSecondOfDay(beginTriggeringTimeCalendar)   /* 时分秒-的天计数秒 */

    /* 修正  triggeringTime  执行时刻 */
    nextCalendar.add(Calendar.SECOND,  beginTriggeringSecondOfDay - getSecondOfDay(nextCalendar))
    nextCalendar
  }

  /**
   * 检测并更新执行表维护的 triggering
   *
   * example-1:
   * false:
   *     last maintenance execute date[2022-03-09 13:28:11],
   *     triggering time [13:30:00],
   *     expect next execute date [2022-03-09 13:30:00],
   *     current time [2022-03-09 13:29:21], will triggering in [39] seconds late
   *
   * example-2:
   * true:
   *     last maintenance execute date[2022-03-09 13:28:11],
   *     triggering time [13:30:00],
   *     expect next execute date [2022-03-09 13:30:00],
   *     current time [2022-03-09 13:30:21]
   *
   * @return
   */
  def checkTriggering(): Boolean = {
    val currentCalendar = Calendar.getInstance
    currentCalendar.setTime(new Date(System.currentTimeMillis()))
    val currentSecondOfDay = getSecondOfDay(currentCalendar)  /* 时分秒-的天计数秒 */

    val nextCalendar = getNextTriggerCalendar

    /* 如果当前时间 大于 期待的下次触发时间 */
    if(currentCalendar.getTimeInMillis > nextCalendar.getTimeInMillis){
      logInfo(s"Iceberg maintenance trigger check return true, last maintenance execute date [$lastExecuteDate], " +
        s"triggering time [$triggeringTime], triggering interval [$triggeringInterval], " +
        s"expect next execute date [${dateFormat.format(nextCalendar.getTime)}], " +
        s"current time [${dateFormat.format(currentCalendar.getTime)}], " +
        s"triggering maintenance iceberg table")
      true
    }else{
      logInfo(s"Iceberg maintenance trigger check return false, last maintenance execute date [$lastExecuteDate], " +
        s"triggering time [$triggeringTime], triggering interval [$triggeringInterval], " +
        s"expect next execute date [${dateFormat.format(nextCalendar.getTime)}], " +
        s"current time [${dateFormat.format(currentCalendar.getTime)}], " +
        s"will triggering in [${getSecondOfDay(nextCalendar)- currentSecondOfDay}] seconds late ")
      false
    }
  }


  /**
   * 获取 Calendar 的 Seconds of Day, 用于判断是否到达每日指定的时间点之后
   * @param calendar Calendar
   * @return
   */
  def getSecondOfDay(calendar: Calendar): Int = {
    calendar.get(Calendar.HOUR_OF_DAY)*60*60 + calendar.get(Calendar.MINUTE)*60 + calendar.get(Calendar.SECOND)
  }
}



object IcebergMaintenance {

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val dayFormat = new SimpleDateFormat("yyyy-MM-dd")
  val timeFormat = new SimpleDateFormat("HH:mm:ss")

  def apply(spark: SparkSession,
            enabled: Boolean,
            triggeringTime: String,
            triggeringInterval:Int,
            props: Properties): IcebergMaintenance = {

    val currentCalendar = Calendar.getInstance
    currentCalendar.setTime(new Date(System.currentTimeMillis()))

    /* 初始化上次执行时间为昨天，执行时刻为 triggeringTime */
    val lastExecuteDate = s"${dayFormat.format(currentCalendar.getTime)} $triggeringTime"

    new IcebergMaintenance(spark, enabled, triggeringTime, triggeringInterval, props, lastExecuteDate)
  }

}