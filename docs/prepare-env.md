
##  运行环境准备

## Kafka 环境
日志解析基于 Debezium 进行

### 配置数据库创建
CREATE DATABASE IF NOT EXISTS streaming;
DROP TABLE IF EXISTS streaming.tbl_streaming_job_conf;
CREATE TABLE streaming.tbl_steaming_job_conf (
ID BIGINT auto_increment NOT NULL COMMENT '记录索引 ID',
BATCH_ID varchar(100) NOT NULL COMMENT '作业批次 ID',
GROUP_ID varchar(100) NOT NULL COMMENT '作业批次下的作业组 ID',
CONF_ID varchar(100) NOT NULL COMMENT '作业组下的作业配置ID',
CONF_VALUE TEXT NULL COMMENT '作业组下的作业配置值',
CREATE_TIME datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
UPDATE_TIME datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
PRIMARY KEY (`ID`),
UNIQUE KEY `CONF_UNIQUE` (`BATCH_ID`,`GROUP_ID`,`CONF_ID`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;CREATE DATABASE IF NOT EXISTS streaming;
DROP TABLE IF EXISTS streaming.tbl_streaming_job_conf;
CREATE TABLE streaming.tbl_steaming_job_conf (
ID BIGINT auto_increment NOT NULL COMMENT '记录索引 ID',
BATCH_ID varchar(100) NOT NULL COMMENT '作业批次 ID',
GROUP_ID varchar(100) NOT NULL COMMENT '作业批次下的作业组 ID',
CONF_ID varchar(100) NOT NULL COMMENT '作业组下的作业配置ID',
CONF_VALUE TEXT NULL COMMENT '作业组下的作业配置值',
CREATE_TIME datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
UPDATE_TIME datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
PRIMARY KEY (`ID`),
UNIQUE KEY `CONF_UNIQUE` (`BATCH_ID`,`GROUP_ID`,`CONF_ID`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;

### 配置示例  
```properties
#############################################################################################
#####                                   Spark Config                                    #####
#############################################################################################
spark.master = yarn
spark.app.name= Kafka2Iceberg
spark.streaming.kafka.maxRatePerPartition = 10000


spark.yarn.jars = hdfs:/user/share/libs/spark/3.2.1/*.jar,hdfs:/user/share/libs/kafka2iceberg/0.1.0/*.jar
spark.sql.sources.partitionOverwriteMode = dynamic
spark.sql.extensions = org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

spark.sql.catalog.hadoop = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hadoop.type= hadoop
spark.sql.catalog.hadoop.warehouse = hdfs://hadoop:8020/user/test/iceberg

spark.sql.warehouse.dir = hdfs://hadoop:8020/user/hive/warehouse
spark.hadoop.hive.metastore.uris = thrift://hadoop:9083


#############################################################################################
#####                                 Hive JDBC Config                                  #####
#############################################################################################
hive.jdbc.url = jdbc:hive2://hadoop:10000
hive.jdbc.user =
hive.jdbc.password =
hive.external.jar = hdfs://hadoop:8020/user/share/libs/iceberg-runtime/0.13.1/iceberg-hive-runtime-0.13.1.jar

#############################################################################################
#####                                Kafka Configs                                      #####
#############################################################################################
kafka.bootstrap.servers=kafka:9092
kafka.schema.registry.url=http://kafka:8081
kafka.consumer.group.id=iceberg-hadoop
kafka.consumer.topic=test.db_gb18030_test.tbl_test_1
kafka.consumer.max.poll.records=5
kafka.consumer.key.deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer
kafka.consumer.value.deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer
kafka.consumer.commit.timeout.millis=60000

# Metadata Columns
record.metadata.source.columns = name, db, table, ts_ms, server_id, file, pos
record.metadata.source.prefix = _src_
record.metadata.transaction.columns = id, total_order, data_collection_order
record.metadata.transaction.prefix = _tsc_
record.metadata.kafka.columns = topic, partition, offset, timestamp
record.metadata.kafka.prefix = _kfk_

#############################################################################################
#####                                Iceberg Configs                                    #####
#############################################################################################
iceberg.table.name = hadoop.db_gb18030_test.tbl_test_1
iceberg.table.partitionBy = id1,id2
iceberg.table.comment = db_gb18030_test tbl_test_1
iceberg.table.properties = 'read.split.target-size'='268435456','write.metadata.delete-after-commit.enabled'='true'
iceberg.table.primaryKey = id1,id2

```