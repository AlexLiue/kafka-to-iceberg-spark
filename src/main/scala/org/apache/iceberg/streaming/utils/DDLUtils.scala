package org.apache.iceberg.streaming.utils

import org.apache.iceberg.streaming.config.{RunCfg, TableCfg}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
 * Iceberg Table DDL Tools
 */
object DDLUtils extends Logging{

  /**
   * 基于 Spark StructType 创建 Iceberg Table 包含 [HadoopCatalog Table] 和 [HiveCatalog Table]
   * @param spark  SparkSession
   * @param structType  StructType
   * @param tableCfg TableCfg
   * @return Create Status
   */
  def createTableIfNotExists(spark: SparkSession,
                             tableCfg: TableCfg,
                             structType: StructType
                            ): Unit = {
    /* create hadoop catalog table */
    val props = tableCfg.getCfgAsProperties
    val icebergTableName = props.getProperty(RunCfg.ICEBERG_TABLE_NAME)
    val partitionBy = props.getProperty(RunCfg.ICEBERG_TABLE_PARTITION_BY)
    val comment = props.getProperty(RunCfg.ICEBERG_TABLE_COMMENT)
    val tblProperties = props.getProperty(RunCfg.ICEBERG_TABLE_PROPERTIES)
    val needCreateTable = HadoopUtils.createHadoopTableIfNotExists(spark,icebergTableName, structType, partitionBy, comment, tblProperties)

    /* if need create hadoop table then, create hive catalog table by external mode */
    if(needCreateTable){
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
