package org.apache.iceberg.streaming.utils


import org.apache.spark.internal.Logging

import java.sql.DriverManager


/**
 *  Hive Catalog 数据处理工具类.
 */
object HiveUtils extends Logging{


  /**
   * 创建 Hive Catalog External Table, 由于 Spark SQL 不支持 STORED BY 语法, 因此使用 hive-jdbc 方式创建 hive 表.
   * @param hiveUrl example: jdbc:hive2://hadoop:10000
   * @param hiveUser 用户
   * @param hivePassword 密码
   * @param extendJar  Iceberg Support Dependencies lib jar
   * @param icebergTableName 表名
   * @param hadoopWarehouse Hadoop 存储目录
   */
  def createOrReplaceHiveTable(
                                  hiveUrl: String,
                                  hiveUser: String,
                                  hivePassword: String,
                                  extendJar: String,
                                  icebergTableName: String,
                                  hadoopWarehouse: String,
                                ): Unit = {

    /* create hive jdbc connection */
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn = DriverManager.getConnection(hiveUrl, hiveUser, hivePassword)

    /* create database */
    val namespace = icebergTableName.split("\\.", 3)(1)
    val createDatabaseDdl = s"create database if not exists $namespace"
    val createDatabaseStmt = conn.prepareStatement(createDatabaseDdl)
    createDatabaseStmt.executeUpdate()
    createDatabaseStmt.close()

    /* create external table */
    val addExtendJar = s"ADD JAR $extendJar"
    val addExtendJarStmt = conn.prepareStatement(addExtendJar)
    addExtendJarStmt.executeUpdate()
    addExtendJarStmt.close()

    val setCatalogType = "SET iceberg.catalog.hadoop.type=hadoop"
    val setCatalogTypeStmt = conn.prepareStatement(setCatalogType)
    setCatalogTypeStmt.executeUpdate()
    setCatalogTypeStmt.close()

    val setHadoopWarehouse = s"SET iceberg.catalog.hadoop.warehouse=$hadoopWarehouse"
    val setWarehouseStmt = conn.prepareStatement(setHadoopWarehouse)
    setWarehouseStmt.executeUpdate()
    setWarehouseStmt.close()

    /* 表如果存在则 drop 删除历史的表 */
    val hiveTableName = icebergTableName.split("\\.",3).drop(1).mkString(".")
    val dropTableDdl = s"DROP TABLE IF EXISTS $hiveTableName"
    logInfo(s"Drop table if exists by sql [$dropTableDdl]")
    val dropTableStmt = conn.prepareStatement(dropTableDdl)
    dropTableStmt.executeUpdate()
    dropTableStmt.close()

    /* 创建 EXTERNAL TABLE  */
    val createTableDdl =
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS $hiveTableName
         |STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
         |TBLPROPERTIES ('iceberg.catalog'='hadoop')
         |""".stripMargin

    logInfo(s"Create external hive catalog table with sql [$createTableDdl]")
    val createTableStmt = conn.prepareStatement(createTableDdl)
    createTableStmt.executeUpdate()
    createTableStmt.close()
    conn.close()
  }
}
