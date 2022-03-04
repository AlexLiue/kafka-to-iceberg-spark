package org.apache.iceberg.streaming.utils

import org.apache.iceberg.mr.hive.HiveIcebergStorageHandler

class HiveUtilsTest extends org.scalatest.FunSuite {

  test("testCreateHiveTableIfNotExists") {
    val hiveUrl = "jdbc:hive2://hadoop:10000"
    val hiveUser = ""
    val hivePassword = ""
    val extendJar = "hdfs://hadoop:8020/user/share/libs/iceberg-runtime/0.13.1/iceberg-hive-runtime-0.13.1.jar"
    val icebergTableName = "hadoop.db_test.tbl_test"
    val hadoopWarehouse = "hdfs://hadoop:8020/user/test/iceberg"

    HiveUtils.createOrReplaceHiveTable(
      hiveUrl, hiveUser, hivePassword,extendJar,
      icebergTableName, hadoopWarehouse,
    )

  }

}
