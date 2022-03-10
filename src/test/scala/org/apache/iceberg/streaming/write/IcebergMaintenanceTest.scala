package org.apache.iceberg.streaming.write

import org.apache.iceberg.streaming.config.RunCfg
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}
import org.apache.iceberg.spark.IcebergSpark

/**
 * HADOOP_USER_NAME=root
 */
class IcebergMaintenanceTest extends org.scalatest.FunSuite {

  test("testCheckTriggering") {
    val maintenance = getMaintenance
    for(i <- 1 until 200){
      val res = maintenance.checkTriggering()
      System.out.println(res)
      Thread.sleep(10000)
    }
  }

  test("testExpireSnapshots") {
    val maintenance = getMaintenance
    var flag = false
    while(!flag){
      flag = maintenance.checkTriggering()
      if(flag){
        maintenance.expireSnapshots()
      }
      Thread.sleep(10000)
    }
  }

  test("compactDataFiles") {
    val maintenance = getMaintenance
    for(i <- 1 until 200){
      if( maintenance.checkTriggering()){
        maintenance.compactDataFiles()
      }
      Thread.sleep(10000)
    }
  }
  test("rewriteManifests") {
    val maintenance = getMaintenance
    for(i <- 1 until 200){
      if( maintenance.checkTriggering()){
        maintenance.rewriteManifests()
      }
      Thread.sleep(10000)
    }
  }


  test("testLaunchMaintenance") {
    val maintenance = getMaintenance
    for(i <- 1 until 200){
      if( maintenance.checkTriggering()){
        maintenance.launchMaintenance()
      }
      Thread.sleep(10000)
    }
  }





  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val dayFormat = new SimpleDateFormat("yyyy-MM-dd")
  val timeFormat = new SimpleDateFormat("HH:mm:ss")

  def getSparkSession: SparkSession = {
    org.apache.spark.sql.SparkSession.builder().
      master("local[3]").
      config("spark.yarn.jars", "hdfs:/user/share/libs/spark/3.2.1/*.jar,hdfs:/user/share/libs/kafka2iceberg/0.1.0/*.jar").
      config("spark.sql.sources.partitionOverwriteMode", "dynamic").
      config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").
      config("spark.sql.catalog.hadoop", "org.apache.iceberg.spark.SparkCatalog").
      config("spark.sql.catalog.hadoop.type", "hadoop").
      config("spark.sql.catalog.hadoop.warehouse", "hdfs://hadoop:8020/user/test/iceberg").
      config("spark.sql.warehouse.dir", "hdfs://hadoop:8020/user/hive/warehouse").
      enableHiveSupport().
      appName("Kafka2Iceberg").getOrCreate()
  }

  def getProperties: Properties = {
    val currentCalendar = Calendar.getInstance
    currentCalendar.setTime(new Date(System.currentTimeMillis()))
    currentCalendar.add(Calendar.SECOND, 20)

    val props = new Properties()
    props.put("iceberg.table.name", "hadoop.db_gb18030_test.tbl_test_1")
    props.put("iceberg.maintenance.enabled", "true")
    props.put("iceberg.maintenance.triggering.time", timeFormat.format(currentCalendar.getTime))
    props.put("iceberg.maintenance.execute.interval", "60000")
    props.put("iceberg.maintenance.snapshot.expire.time", "86400000")
    props.put("iceberg.maintenance.compact.filter.column", "_src_ts_ms")
    props.put("iceberg.maintenance.compact.day.offset", "0")
    props.put("iceberg.maintenance.compact.target.file.size.bytes", "134217728")
    props.put("iceberg.maintenance.manifests.file.length", "10485760")
    props
  }


  def getMaintenance: IcebergMaintenance = {
    val spark = getSparkSession
    val props = getProperties
    val triggeringTime = props.getProperty(RunCfg.ICEBERG_MAINTENANCE_TRIGGERING_TIME)
    val executeInterval =  props.getProperty(RunCfg.ICEBERG_MAINTENANCE_EXECUTE_INTERVAL).toInt
    val currentCalendar = Calendar.getInstance
    currentCalendar.setTime(new Date(System.currentTimeMillis()))
    currentCalendar.add (Calendar.MILLISECOND, -40000)
    val lastExecuteDate = dateFormat.format(currentCalendar.getTime)
    IcebergMaintenance(spark, enabled = true, triggeringTime, executeInterval, props, lastExecuteDate)
  }

}
