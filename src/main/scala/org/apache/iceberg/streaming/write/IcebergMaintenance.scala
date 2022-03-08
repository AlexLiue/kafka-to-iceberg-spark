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
 * @param triggeringTime   执行  Iceberg 表维护操作的时间（24小格式: 如 21:00:00 ）
 * @param executeInterval  执行  Iceberg 表维护操作的时间间隔，单位秒 （格式: 86400000 (即 24小时)）
 * @param lastExecuteDate  执行  Iceberg 表维护操作的历史时间，格式日期: 2021-12-23 21:00:00
 *
 */
case class IcebergMaintenance (
                               spark: SparkSession,
                               enabled: Boolean,
                               triggeringTime: String,
                               executeInterval: Int,
                               props: Properties,
                               lastExecuteDate: String,
                               triggeringFlag: Boolean,
                        )extends Logging{

  def updateAndResetTrigger(): IcebergMaintenance = {
    val lastDate = dateFormat.parse(lastExecuteDate)
    val lastCalendar = Calendar.getInstance
    lastCalendar.setTime(lastDate)
    lastCalendar.add(executeInterval, Calendar.MILLISECOND)

    IcebergMaintenance(spark, enabled, triggeringTime, executeInterval, props,
      dateFormat.format(lastCalendar.getTime), triggeringFlag = false)
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
    val tsToExpire =  props.getProperty(RunCfg.ICEBERG_MAINTENANCE_SNAPSHOT_EXPIRE_TIME)
    if(tsToExpire != null){
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

      logInfo(s"Iceberg maintenance expire snapshots with expire time [${tsToExpire.toLong}]")
      SparkActions
        .get()
        .expireSnapshots(table)
        .expireOlderThan(tsToExpire.toLong)
        .execute()
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
    lastCalendar.add(dayOffset, Calendar.DAY_OF_YEAR)

    /* filter lower bound */
    val lowerStr = s"${dayFormat.format(lastCalendar.getTime)} 00:00:00"
    val lower = dateFormat.parse(lowerStr).getTime

    /* filter upper bound */
    lastCalendar.add(executeInterval, Calendar.MILLISECOND)
    val upperStr = s"${dayFormat.format(lastCalendar.getTime)} 00:00:00"
    val upper = dateFormat.parse(s"${dayFormat.format(lastCalendar.getTime)} 00:00:00").getTime
    logInfo(s"Iceberg maintenance compact data files, with data column [filterColumn] from [$lowerStr] to [$upperStr], target file size [$targetFileSize]")
    SparkActions
      .get()
      .rewriteDataFiles(table)
      .filter(Expressions.and(
        Expressions.greaterThanOrEqual(filterColumn, lower),
        Expressions.lessThan(filterColumn, upper)))
      .option("target-file-size-bytes", targetFileSize)
      .execute()
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

    logInfo(s"Iceberg maintenance rewrite manifests to max length [$manifestsFileLength]")
    SparkActions
      .get()
      .rewriteManifests(table).rewriteIf(x => x.length() > manifestsFileLength)
      .execute()
  }




  /**
   * 检测并更新执行表维护的 triggering
   * @return
   */
  def checkTriggering(): Boolean = {

    val lastDate = dateFormat.parse(lastExecuteDate)
    val lastCalendar = Calendar.getInstance
    lastCalendar.setTime(lastDate)

    val currentCalendar = Calendar.getInstance
    currentCalendar.setTime(new Date(System.currentTimeMillis()))
    val currentSecondOfDay = getSecondOfDay(currentCalendar)  /* 时分秒-的天计数秒 */

    val nextCalendar = Calendar.getInstance
    nextCalendar.setTime(lastDate)
    nextCalendar.add(executeInterval, Calendar.MILLISECOND)  /* 上次执行时间 +  执行间隔 */

    val triggeringTimeCalendar =  Calendar.getInstance
    triggeringTimeCalendar.setTime(timeFormat.parse(triggeringTime))
    val triggeringSecondOfDay = getSecondOfDay(triggeringTimeCalendar)   /* 时分秒-的天计数秒 */

    /* 如果当前时间 大于 期待的下次触发时间 */
    if(currentCalendar.getTimeInMillis > nextCalendar.getTimeInMillis){

       /* 如果当前 时间 且大于 triggeringTime 时间点 返回 true  */
      if(currentSecondOfDay > triggeringSecondOfDay){
        logInfo(s"Iceberg maintenance trigger check return true, last maintenance execute date[$lastExecuteDate], " +
          s"expect next execute date [${dateFormat.format(nextCalendar.getTime)}]")
        IcebergMaintenance(spark, enabled, triggeringTime, executeInterval, props, lastExecuteDate, triggeringFlag = true)
        true
      }else{
        logInfo(s"Iceberg maintenance trigger check return false, last maintenance execute date[$lastExecuteDate], " +
          s"expect next execute date [${dateFormat.format(nextCalendar.getTime)}]," +
          s"will triggering in [${triggeringSecondOfDay - currentSecondOfDay}] seconds late "
        )
        false
      }
    }else{
      logInfo(s"Iceberg maintenance trigger check return false, last maintenance execute date[$lastExecuteDate], " +
        s"expect next execute date [${dateFormat.format(nextCalendar.getTime)}]," +
        s"will triggering in [${Math.max(
          nextCalendar.getTimeInMillis - currentCalendar.getTimeInMillis,
          triggeringSecondOfDay - currentSecondOfDay)}] seconds late ")
      false
    }
  }


  /**
   * 获取 Calendar 的 Seconds of Day, 用于判断是否到达每日指定的时间点之后
   * @param calendar Calendar
   * @return
   */
  def getSecondOfDay(calendar: Calendar): Long = {
     calendar.get(Calendar.HOUR_OF_DAY)*60*60 + calendar.get(Calendar.MINUTE)*60 + calendar.get(Calendar.SECOND)
  }


}







object IcebergMaintenance {

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val dayFormat = new SimpleDateFormat("yyyy-MM-dd")
  val timeFormat = new SimpleDateFormat("HH:mm:ss")

  def apply( spark: SparkSession,
             enabled: Boolean,
             triggeringTime: String,
             executeInterval:Int,
             props: Properties): IcebergMaintenance = {

    val currentCalendar = Calendar.getInstance
    currentCalendar.setTime(new Date(System.currentTimeMillis()))
    currentCalendar.add(-1, Calendar.DAY_OF_YEAR)

    /* 初始化上次执行时间为昨天，执行时刻为 triggeringTime */
    val lastExecuteDate = s"dayFormat.format(currentCalendar.getTime) $triggeringTime"

    new IcebergMaintenance(spark, enabled, triggeringTime, executeInterval, props, lastExecuteDate, false)
  }

}
