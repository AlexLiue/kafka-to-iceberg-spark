package org.apache.iceberg.streaming.utils

import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.types.Types.NestedField
import org.apache.iceberg.{Schema, UpdateSchema}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`


/**
 *  Hadoop Catalog 数据处理工具类.
 */
object HadoopUtils extends Logging {

  /**
   *
   * 返回值：
   * True: 表不存在
   */

  /**
   * 创建 HadoopCatalog Table.
   * 根据返回值经判断是否需要建立 Hive External Table
   *
   * @param spark            SparkSession
   * @param icebergTableName icebergTableName
   * @param structType       Spark DataFrame Schema StructType
   * @param partitionBy      partitionBy
   * @param comment          comment
   * @param tblProperties    tblProperties
   * @return True(表不存在,创建 HadoopCatalog Table), False(表已存在)
   */
  def createHadoopTableIfNotExists(spark: SparkSession,
                                   icebergTableName: String,
                                   structType: StructType,
                                   partitionBy: String,
                                   comment: String,
                                   tblProperties: String
                                  ): Boolean = {
    val tableItems = icebergTableName.split("\\.", 3)
    val namespace = tableItems(1)
    val tableName = tableItems(2)

    /* 创建 HadoopCatalog 对象 */
    val hadoopCatalog = new HadoopCatalog()
    hadoopCatalog.setConf(spark.sparkContext.hadoopConfiguration); // Configure using Spark's Hadoop configuration
    val properties = new util.HashMap[String, String]()
    properties.put("warehouse", spark.conf.get("spark.sql.catalog.hadoop.warehouse"))
    hadoopCatalog.initialize("hadoop", properties)

    /* 如果表不存在则创建表 */
    val tableIdentifier = TableIdentifier.of(namespace, tableName)
    if (!hadoopCatalog.tableExists(tableIdentifier)) {
      /* 如果 namespace 不存在则创建  namespace  */
      if (!hadoopCatalog.namespaceExists(Namespace.of(namespace))) {
        logInfo(s"Hadoop catalog [$hadoopCatalog], namespace [$namespace] not exists, try to create namespace")
        hadoopCatalog.createNamespace(Namespace.of(namespace))
      } else {
        logInfo(s"Hadoop catalog [$hadoopCatalog], namespace [$namespace] exists")
      }
      val icebergSchema = SparkSchemaUtil.convert(structType)
      val columnList = icebergSchema.columns().map(c => s"${c.name()} ${c.`type`()}").mkString(", ")

      val createDDL =
        s"""
           |CREATE TABLE $icebergTableName (
           |$columnList)
           |USING iceberg
           |PARTITIONED BY ($partitionBy)
           |COMMENT '$comment'
           |TBLPROPERTIES ($tblProperties)
           |""".stripMargin

      logInfo(s"Hadoop table not exists, start create namespace[$namespace]-table[$icebergTableName] with sql [$createDDL]")
      spark.sql(createDDL)
      hadoopCatalog.close()
      true
    } else {
      logInfo("Hadoop table is early exist, ignore create table ")
      false
    }
  }


  /**
   * 使用 检测表结构 更新 - 如果 mergeFlag 为 true 则将表结构更新为合并后的表结构
   *
   * @param spark            SparkSession
   * @param icebergTableName iceberg table name
   * @param curSchema        Avro Schema
   * @param enableDropColumn 如果为 true 则 Drop 删除的列， 否则且抛出异常
   * @return 表结构更新检测结构,用于返回判断是否需要更新重建 Hive 表: false-Schema未更新, true-Schema发生了更新
   *
   */
  def checkAndAlterHadoopTableSchema(spark: SparkSession,
                                     icebergTableName: String,
                                     curSchema: org.apache.avro.Schema,
                                     enableDropColumn: Boolean = false): Boolean = {

    val tableItems = icebergTableName.split("\\.", 3)
    val namespace = tableItems(1)
    val tableName = tableItems(2)

    /* 创建 HadoopCatalog 对象 */
    val catalog = new HadoopCatalog()
    catalog.setConf(spark.sparkContext.hadoopConfiguration)
    val properties = new util.HashMap[String, String]()
    properties.put("warehouse", spark.conf.get("spark.sql.catalog.hadoop.warehouse"))
    catalog.initialize("hadoop", properties)

    /* 读取加载 Catalog Table */
    val tableIdentifier = TableIdentifier.of(namespace, tableName)
    val catalogTable = catalog.loadTable(tableIdentifier)
    /* 修复 IcebergSchema 的起始ID (使用 AvroSchemaUtil.toIceberg() 函数产生的Schema 从 0 计数, 而 Iceberg 的 Schema 从 1 开始计数) */
    val curIcebergSchema = copySchemaWithStartId(AvroSchemaUtil.toIceberg(curSchema), startId = 1, lowerCase = true)
    val catalogTableSchema = copyFilterMetadataColumns(catalogTable.schema(), 1, lowerCase = true)

    if (!curIcebergSchema.sameSchema(catalogTableSchema)) {
      logInfo(s"Table [$icebergTableName] schema changed, before [${catalogTable.schema().toString}]")
      val perColumns = catalogTable.schema().columns()
      val curColumns = curIcebergSchema.columns().map(c => NestedField.optional(c.fieldId(), c.name().toLowerCase(), c.`type`(), c.doc()))
      val perColumnNames = perColumns.map(column => column.name()).toList
      val curColumnNames = curColumns.map(column => column.name()).toList

      /* Step 0 : 创建 UpdateSchema 对象 */
      var updateSchema: UpdateSchema = catalogTable.updateSchema()

      /* Step 1 : 添加列 (使用 unionByNameWith 进行合并) */
      updateSchema = updateSchema.unionByNameWith(curIcebergSchema)

      /* Step 2 : 删除列 （基于列名 drop 被删除的列), filter 过滤掉 定义的 metadata 列（以 _ 开头） */
      val deleteColumnNames = perColumnNames.diff(curColumnNames).filter(!_.startsWith("_"))
      if (deleteColumnNames.nonEmpty) {
        if (enableDropColumn) {
          for (name <- deleteColumnNames) {
            updateSchema = updateSchema.deleteColumn(name)
          }
        } else {
          throw new RuntimeException("")
        }
      }

      /* Step 3 : 调整列顺序  */
      val lastMetadataColumn = perColumnNames.filter(_.startsWith("_")).last
      for (i <- curColumnNames.indices) {
        if (i == 0) {
          updateSchema = updateSchema.moveAfter(curColumnNames(i), lastMetadataColumn)
        } else {
          updateSchema = updateSchema.moveAfter(curColumnNames(i), curColumnNames(i - 1))
        }
      }

      /* Step 3 : 提交执行 Schema 更新  */
      logInfo(s"Try to alter table to ${updateSchema.apply().toString}")
      updateSchema.commit()
      logInfo(s"Table [$icebergTableName] schema changed success ")
      catalog.close()
      true
    } else {
      logInfo(s"Table [$icebergTableName] schema changed did not changed")
      false
    }
  }


  /**
   * Cupy 修改 Iceberg Schema 的起始ID.
   * (使用 AvroSchemaUtil.toIceberg() 函数产生的Schema 从 0 计数, 而 Iceberg 的 Schema 从 1 开始计数
   *
   * @param schema    Schema
   * @param startId   start index id
   * @param lowerCase column past to lowerCase 列名是否小写
   * @return Schema
   */
  def copySchemaWithStartId(schema: Schema, startId: Int = 0, lowerCase: Boolean): Schema = {
    val columns = schema.columns()
    val newColumns = new util.ArrayList[NestedField](columns.size())
    for (i <- 0 until columns.size()) {
      val column = columns.get(i)
      if (lowerCase) {
        newColumns.add(NestedField.optional(startId + i, column.name().toLowerCase, column.`type`(), column.doc()))
      } else {
        newColumns.add(NestedField.optional(startId + i, column.name(), column.`type`(), column.doc()))
      }
    }
    new Schema(newColumns)
  }

  /**
   * Cupy 过滤 Iceberg Schema 的 Metadata 列信息. 用于与当前的 Schema 进行对比判断，是否需要更新表结构.
   *
   * @param schema    Schema
   * @param startId   结构 Schema 的开始 id
   * @param lowerCase 列名是否小写
   * @return Schema
   */
  def copyFilterMetadataColumns(schema: Schema, startId: Int = 0, lowerCase: Boolean): Schema = {
    val columns = schema.columns()
    val newColumns = new util.ArrayList[NestedField](columns.size())
    val firstIndex = columns.count(_.name().startsWith("_")) /* 第一个 非 Metadata 的列 */
    for (i <- firstIndex until columns.size()) {
      val column = columns.get(i)
      if (lowerCase) {
        newColumns.add(NestedField.optional(i - firstIndex + startId, column.name().toLowerCase, column.`type`(), column.doc()))
      } else {
        newColumns.add(NestedField.optional(i - firstIndex + startId, column.name(), column.`type`(), column.doc()))
      }
    }
    new Schema(newColumns)
  }
}