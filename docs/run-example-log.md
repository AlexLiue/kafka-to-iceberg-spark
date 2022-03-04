

# 执行日志示例  

## 运行配置示范
```text
## Spark Configs

spark.master = local[2]
spark.app.name= Kafka2Iceberg

spark.sql.sources.partitionOverwriteMode = dynamic
spark.sql.extensions = org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.hive = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hive.type = hive
spark.hadoop.hive.metastore.uris = thrift://hadoop:9083
spark.sql.warehouse.dir = hdfs://hadoop:8020/user/hive/warehouse
spark.streaming.kafka.maxRatePerPartition=10000


# Kafka Configs
kafka.bootstrap.servers=kafka:9092
kafka.schema.registry.url=http://kafka:8081
kafka.consumer.group.id=g1-3
kafka.consumer.topic=test.db_gb18030_test.tbl_test
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

# Iceberg Configs
iceberg.table.name = hive.db_gb18030_test.tbl_test
iceberg.table.partitionBy = c1
iceberg.table.location = hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test
iceberg.table.comment = db_gb18030_test tbl_test
iceberg.table.properties = 'read.split.target-size'='268435456'

iceberg.table.primaryKey = c1,c2

```
##  初始化 StatusAccumulator  /  SchemaBroadcast
```text
22/02/28 20:36:19 INFO Kafka2Iceberg: After Init StatusAccumulator [Map(hive.db_gb18030_test.tbl_test -> StatusAccumulator(Map(test.db_gb18030_test.tbl_test:0 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 0, range: [0 -> 0], curOffset: 0), test.db_gb18030_test.tbl_test:1 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 1, range: [0 -> 0], curOffset: 0), test.db_gb18030_test.tbl_test:2 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 2, range: [0 -> 0], curOffset: 0)), 1))]
22/02/28 20:36:19 INFO Kafka2Iceberg: After Init SchemaBroadcast   [List(SchemaBroadcast(hive.db_gb18030_test.tbl_test, SchemaBroadcast(Map({"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"} -> 2, {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"} -> 1), Map(1 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"}, 2 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"}))))]
```

## 数据内容 
```text
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+----------------------------------------+----------------+--------------------------+-----------------------------+--------------+-----------+--------------+---+---+---+---+---+----+-------------+-------------+
|_src_name|_src_db        |_src_table|_src_ts_ms   |_src_server_id|_src_file       |_src_pos|_src_op|_src_ts_ms_r |_tsc_id                                 |_tsc_total_order|_tsc_data_collection_order|_kfk_topic                   |_kfk_partition|_kfk_offset|_kfk_timestamp|id |c1 |c2 |c3 |c4 |c5  |create_time  |update_time  |
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+----------------------------------------+----------------+--------------------------+-----------------------------+--------------+-----------+--------------+---+---+---+---+---+----+-------------+-------------+
|test     |db_gb18030_test|tbl_test  |1645690414000|1             |mysql-bin.000010|8775    |u      |1645690414404|e45b718e-906f-11ec-89e3-0242c0a8640a:219|1               |1                         |test.db_gb18030_test.tbl_test|0             |12         |1645690414488 |1  |v1 |v2 |5  |12 |null|1645157193000|1645690414000|
|test     |db_gb18030_test|tbl_test  |1645690498000|1             |mysql-bin.000010|9164    |c      |1645690498874|e45b718e-906f-11ec-89e3-0242c0a8640a:220|1               |1                         |test.db_gb18030_test.tbl_test|1             |0          |1645690499261 |2  |A1 |A2 |1  |1  |null|1645157193000|1645690414000|
|test     |db_gb18030_test|tbl_test  |1645895249000|1             |mysql-bin.000014|6745    |u      |1645895249588|e45b718e-906f-11ec-89e3-0242c0a8640a:234|1               |1                         |test.db_gb18030_test.tbl_test|1             |1          |1645895249714 |2  |A1 |A2 |2  |1  |null|1645157193000|1645895249000|
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+----------------------------------------+----------------+--------------------------+-----------------------------+--------------+-----------+--------------+---+---+---+---+---+----+-------------+-------------+

```

## 自动创建表
```text
22/02/28 20:36:33 INFO DDLHelper: start create namespace[db_gb18030_test]-table[hive.db_gb18030_test.tbl_test] with sql [
CREATE TABLE hive.db_gb18030_test.tbl_test (
_src_name string, _src_db string, _src_table string, _src_ts_ms long, _src_server_id long, _src_file string, _src_pos long, _src_op string, _src_ts_ms_r long, _tsc_id string, _tsc_total_order long, _tsc_data_collection_order long, _kfk_topic string, _kfk_partition int, _kfk_offset long, _kfk_timestamp long, id int, c1 string, c2 string, c3 int, c4 long, create_time long, update_time long)
USING iceberg
PARTITIONED BY (c1)
LOCATION 'hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test'
COMMENT 'db_gb18030_test tbl_test'
TBLPROPERTIES ('read.split.target-size'='268435456')
]
22/02/28 20:36:33 INFO metastore: Trying to connect to metastore with URI thrift://hadoop:9083
22/02/28 20:36:33 INFO metastore: Opened a connection to metastore, current connections: 1
22/02/28 20:36:33 INFO metastore: Connected to metastore.
22/02/28 20:36:33 INFO BaseMetastoreTableOperations: Successfully committed to table hive.db_gb18030_test.tbl_test in 382 ms
22/02/28 20:36:34 INFO BaseMetastoreTableOperations: Refreshing table metadata from new version: hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test/metadata/00000-c5933bd0-7811-4e7b-b5ee-61f33bdbd75b.metadata.json
22/02/28 20:36:34 INFO RowLevelCommandScanRelationPushDown: 
Pushing operators to hive.db_gb18030_test.tbl_test
Pushed filters: 
Filters that were not pushed: 
Output: _src_name#176, _src_db#177, _src_table#178, _src_ts_ms#179L, _src_server_id#180L, _src_file#181, _src_pos#182L, _src_op#183, _src_ts_ms_r#184L, _tsc_id#185, _tsc_total_order#186L, _tsc_data_collection_order#187L, _kfk_topic#188, _kfk_partition#189, _kfk_offset#190L, _kfk_timestamp#191L, id#192, c1#193, c2#194, c3#195, c4#196L, create_time#197L, update_time#198L, _file#201

```


## Kafka 数据读取 / 丢弃 / 手动 Offset 提交  
```text
22/02/28 20:36:30 INFO SubscriptionState: [Consumer clientId=consumer-g1-3-3, groupId=g1-3] Resetting offset for partition test.db_gb18030_test.tbl_test-2 to position FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}}.
22/02/28 20:36:30 INFO SubscriptionState: [Consumer clientId=consumer-g1-3-3, groupId=g1-3] Resetting offset for partition test.db_gb18030_test.tbl_test-0 to position FetchPosition{offset=13, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}}.
22/02/28 20:36:30 INFO SubscriptionState: [Consumer clientId=consumer-g1-3-3, groupId=g1-3] Resetting offset for partition test.db_gb18030_test.tbl_test-1 to position FetchPosition{offset=2, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}}.
22/02/28 20:36:30 INFO JobScheduler: Added jobs for time 1646051790000 ms
22/02/28 20:36:30 INFO JobScheduler: Starting job streaming job 1646051790000 ms.0 from job set of time 1646051790000 ms
22/02/28 20:36:30 INFO Kafka2Iceberg: ----------------------------------------------------------------------------------
22/02/28 20:36:30 INFO Kafka2Iceberg: OffsetRanges: [OffsetRange(topic: 'test.db_gb18030_test.tbl_test', partition: 0, range: [0 -> 13]),OffsetRange(topic: 'test.db_gb18030_test.tbl_test', partition: 2, range: [0 -> 0]),OffsetRange(topic: 'test.db_gb18030_test.tbl_test', partition: 1, range: [0 -> 2])]
22/02/28 20:36:30 INFO Kafka2Iceberg: partitionOffsets: [test.db_gb18030_test.tbl_test:0 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 0, range: [0 -> 13], curOffset: 0),test.db_gb18030_test.tbl_test:1 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 1, range: [0 -> 2], curOffset: 0),test.db_gb18030_test.tbl_test:2 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 2, range: [0 -> 0], curOffset: 0)]
22/02/28 20:36:30 INFO IcebergWriter: beginning write data into hive.db_gb18030_test.tbl_test ...
22/02/28 20:36:35 INFO DAGScheduler: Job 6 finished: sql at IcebergWriter.scala:60, took 0.777342 s
22/02/28 20:36:35 INFO ReplaceDataExec: Data source write support IcebergBatchWrite(table=hive.db_gb18030_test.tbl_test, format=PARQUET) is committing.
22/02/28 20:36:35 INFO SparkWrite: Committing overwrite of 0 data files with 1 new data files, scanSnapshotId: null, conflictDetectionFilter: true to table hive.db_gb18030_test.tbl_test
22/02/28 20:36:36 INFO BaseMetastoreTableOperations: Successfully committed to table hive.db_gb18030_test.tbl_test in 140 ms
22/02/28 20:36:36 INFO SnapshotProducer: Committed snapshot 8977969099846765012 (BaseOverwriteFiles)
22/02/28 20:36:36 INFO BaseMetastoreTableOperations: Refreshing table metadata from new version: hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test/metadata/00001-d4b5d27b-a4d6-46e8-a131-b7ea8a6b5eea.metadata.json
22/02/28 20:36:36 INFO SparkWrite: Committed in 839 ms
22/02/28 20:36:36 INFO ReplaceDataExec: Data source write support IcebergBatchWrite(table=hive.db_gb18030_test.tbl_test, format=PARQUET) committed.
22/02/28 20:36:36 INFO IcebergWriter: finished write data into hive.db_gb18030_test.tbl_test ...
22/02/28 20:36:36 INFO Kafka2Iceberg: commit offset rangers: [OffsetRange(topic: 'test.db_gb18030_test.tbl_test', partition: 0, range: [0 -> 12]),OffsetRange(topic: 'test.db_gb18030_test.tbl_test', partition: 1, range: [0 -> 0]),OffsetRange(topic: 'test.db_gb18030_test.tbl_test', partition: 2, range: [0 -> 0])]

```

## Schema 更新检测 与 Steaming 作业重启 
```text
22/02/28 20:36:36 INFO JobScheduler: Finished job streaming job 1646051790000 ms.0 from job set of time 1646051790000 ms
22/02/28 20:36:36 ERROR JobScheduler: Error running job streaming job 1646051790000 ms.0
org.apache.iceberg.streaming.exception.SchemaChangedException: detect schema version changed, need restart SparkStream job...
	at org.apache.iceberg.streaming.Kafka2Iceberg$.$anonfun$startStreamJob$1(Kafka2Iceberg.scala:210)
	at org.apache.iceberg.streaming.Kafka2Iceberg$.$anonfun$startStreamJob$1$adapted(Kafka2Iceberg.scala:182)
	at org.apache.spark.streaming.dstream.DStream.$anonfun$foreachRDD$2(DStream.scala:629)
	at org.apache.spark.streaming.dstream.DStream.$anonfun$foreachRDD$2$adapted(DStream.scala:629)
	at org.apache.spark.streaming.dstream.ForEachDStream.$anonfun$generateJob$2(ForEachDStream.scala:51)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
	at org.apache.spark.streaming.dstream.DStream.createRDDWithLocalProperties(DStream.scala:417)
	at org.apache.spark.streaming.dstream.ForEachDStream.$anonfun$generateJob$1(ForEachDStream.scala:51)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
	at scala.util.Try$.apply(Try.scala:213)
	at org.apache.spark.streaming.scheduler.Job.run(Job.scala:39)
	at org.apache.spark.streaming.scheduler.JobScheduler$JobHandler.$anonfun$run$1(JobScheduler.scala:256)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
	at scala.util.DynamicVariable.withValue(DynamicVariable.scala:62)
	at org.apache.spark.streaming.scheduler.JobScheduler$JobHandler.run(JobScheduler.scala:256)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
22/02/28 20:36:36 WARN Kafka2Iceberg: Detect Schema Version Changed, Restart SparkStream Job...
22/02/28 20:36:36 INFO ReceiverTracker: ReceiverTracker stopped
22/02/28 20:36:36 INFO JobGenerator: Stopping JobGenerator immediately
22/02/28 20:36:36 INFO RecurringTimer: Stopped timer for JobGenerator after time 1646051790000
22/02/28 20:36:36 INFO ConsumerCoordinator: [Consumer clientId=consumer-g1-3-3, groupId=g1-3] Revoke previously assigned partitions test.db_gb18030_test.tbl_test-2, test.db_gb18030_test.tbl_test-0, test.db_gb18030_test.tbl_test-1
22/02/28 20:36:36 INFO AbstractCoordinator: [Consumer clientId=consumer-g1-3-3, groupId=g1-3] Member consumer-g1-3-3-22fcf88b-3260-4d1e-819d-b7adb0c7c180 sending LeaveGroup request to coordinator 192.168.100.30:9092 (id: 2147483647 rack: null) due to the consumer is being closed
22/02/28 20:36:36 INFO Metrics: Metrics scheduler closed
22/02/28 20:36:36 INFO Metrics: Closing reporter org.apache.kafka.common.metrics.JmxReporter
22/02/28 20:36:36 INFO Metrics: Metrics reporters closed
22/02/28 20:36:36 INFO AppInfoParser: App info kafka.consumer for consumer-g1-3-3 unregistered
22/02/28 20:36:36 INFO JobGenerator: Stopped JobGenerator
22/02/28 20:36:36 INFO JobScheduler: Stopped JobScheduler
22/02/28 20:36:36 INFO StreamingContext: StreamingContext stopped successfully
22/02/28 20:36:39 INFO Kafka2Iceberg: commit kafka offset [test.db_gb18030_test.tbl_test-2 -> OffsetAndMetadata{offset=0, leaderEpoch=null, metadata=''},test.db_gb18030_test.tbl_test-0 -> OffsetAndMetadata{offset=12, leaderEpoch=null, metadata=''},test.db_gb18030_test.tbl_test-1 -> OffsetAndMetadata{offset=0, leaderEpoch=null, metadata=''}]
22/02/28 20:36:43 INFO BaseMetastoreTableOperations: Refreshing table metadata from new version: hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test/metadata/00001-d4b5d27b-a4d6-46e8-a131-b7ea8a6b5eea.metadata.json
22/02/28 20:36:43 INFO BaseMetastoreCatalog: Table loaded by catalog: hive.db_gb18030_test.tbl_test
22/02/28 20:36:43 INFO DDLHelper: table [hive.db_gb18030_test.tbl_test] schema changed, before [table {
  1: _src_name: optional string
  2: _src_db: optional string
  3: _src_table: optional string
  4: _src_ts_ms: optional long
  5: _src_server_id: optional long
  6: _src_file: optional string
  7: _src_pos: optional long
  8: _src_op: optional string
  9: _src_ts_ms_r: optional long
  10: _tsc_id: optional string
  11: _tsc_total_order: optional long
  12: _tsc_data_collection_order: optional long
  13: _kfk_topic: optional string
  14: _kfk_partition: optional int
  15: _kfk_offset: optional long
  16: _kfk_timestamp: optional long
  17: id: optional int
  18: c1: optional string
  19: c2: optional string
  20: c3: optional int
  21: c4: optional long
  22: create_time: optional long
  23: update_time: optional long
}]
22/02/28 20:36:43 INFO BaseMetastoreTableOperations: Successfully committed to table hive.db_gb18030_test.tbl_test in 179 ms
22/02/28 20:36:43 INFO BaseMetastoreTableOperations: Refreshing table metadata from new version: hdfs://hadoop:8020/user/hive/warehouse/db_gb18030_test.db/tbl_test/metadata/00002-751f9aac-1300-4185-b347-659be2063321.metadata.json
22/02/28 20:36:43 INFO BaseMetastoreCatalog: Table loaded by catalog: hive.db_gb18030_test.tbl_test
22/02/28 20:36:43 INFO DDLHelper: table [hive.db_gb18030_test.tbl_test] schema changed, after  [table {
  1: _src_name: optional string
  2: _src_db: optional string
  3: _src_table: optional string
  4: _src_ts_ms: optional long
  5: _src_server_id: optional long
  6: _src_file: optional string
  7: _src_pos: optional long
  8: _src_op: optional string
  9: _src_ts_ms_r: optional long
  10: _tsc_id: optional string
  11: _tsc_total_order: optional long
  12: _tsc_data_collection_order: optional long
  13: _kfk_topic: optional string
  14: _kfk_partition: optional int
  15: _kfk_offset: optional long
  16: _kfk_timestamp: optional long
  17: id: optional int
  18: c1: optional string
  19: c2: optional string
  20: c3: optional int
  21: c4: optional long
  24: c5: optional string
  22: create_time: optional long
  23: update_time: optional long
}]
22/02/28 20:36:43 INFO Kafka2Iceberg: After Init StatusAccumulator [Map(hive.db_gb18030_test.tbl_test -> StatusAccumulator(Map(test.db_gb18030_test.tbl_test:0 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 0, range: [12 -> 0], curOffset: 12), test.db_gb18030_test.tbl_test:1 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 1, range: [0 -> 0], curOffset: 0), test.db_gb18030_test.tbl_test:2 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test', partition: 2, range: [0 -> 0], curOffset: 0)), 2))]
22/02/28 20:36:43 INFO Kafka2Iceberg: After Init SchemaBroadcast   [List(SchemaBroadcast(hive.db_gb18030_test.tbl_test, SchemaBroadcast(Map({"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"} -> 2, {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"} -> 1), Map(1 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"}, 2 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID","type":"int"},{"name":"C1","type":["null","string"],"default":null},{"name":"C2","type":{"type":"string","connect.default":"CV2"},"default":"CV2"},{"name":"C3","type":["null","int"],"default":null},{"name":"C4","type":["null","long"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test.Envelope"}))))]
22/02/28 20:36:43 INFO Kafka2Iceberg: Restarted StreamingContext ...

```

## 
``
## 作业运行详细日志示例  
```
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+-----------------------------------------+----------------+--------------------------+-------------------------------+--------------+-----------+--------------+----+---+----+----+-------------+-------------+
|_src_name|_src_db        |_src_table|_src_ts_ms   |_src_server_id|_src_file       |_src_pos|_src_op|_src_ts_ms_r |_tsc_id                                  |_tsc_total_order|_tsc_data_collection_order|_kfk_topic                     |_kfk_partition|_kfk_offset|_kfk_timestamp|id1 |id2|c1  |c2  |create_time  |update_time  |
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+-----------------------------------------+----------------+--------------------------+-------------------------------+--------------+-----------+--------------+----+---+----+----+-------------+-------------+
|test     |db_gb18030_test|tbl_test_1|1646128660000|1             |mysql-bin.000019|22376   |u      |1646128660792|e45b718e-906f-11ec-89e3-0242c0a8640a:1149|1               |1                         |test.db_gb18030_test.tbl_test_1|1             |2          |1646128660882 |1002|A  |V2-1|9013|1646101923000|1646128660000|
|test     |db_gb18030_test|tbl_test_1|1646128132000|1             |mysql-bin.000019|18653   |u      |1646128132882|e45b718e-906f-11ec-89e3-0242c0a8640a:1142|1               |1                         |test.db_gb18030_test.tbl_test_1|2             |2          |1646128132918 |1001|A  |V1-1|8012|1646101923000|1646128132000|
|test     |db_gb18030_test|tbl_test_1|1646128650000|1             |mysql-bin.000019|22036   |c      |1646128660774|e45b718e-906f-11ec-89e3-0242c0a8640a:1148|1               |1                         |test.db_gb18030_test.tbl_test_1|2             |3          |1646128660882 |1003|A  |V3-1|3013|1646101923000|1646128132000|
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+-----------------------------------------+----------------+--------------------------+-------------------------------+--------------+-----------+--------------+----+---+----+----+-------------+-------------+

+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+-----------------------------------------+----------------+--------------------------+-------------------------------+--------------+-----------+--------------+----+---+----+-----+----+-------------+-------------+
|_src_name|_src_db        |_src_table|_src_ts_ms   |_src_server_id|_src_file       |_src_pos|_src_op|_src_ts_ms_r |_tsc_id                                  |_tsc_total_order|_tsc_data_collection_order|_kfk_topic                     |_kfk_partition|_kfk_offset|_kfk_timestamp|id1 |id2|c1  |c2   |c3  |create_time  |update_time  |
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+-----------------------------------------+----------------+--------------------------+-------------------------------+--------------+-----------+--------------+----+---+----+-----+----+-------------+-------------+
|test     |db_gb18030_test|tbl_test_1|1646129902000|1             |mysql-bin.000019|28822   |u      |1646129902778|e45b718e-906f-11ec-89e3-0242c0a8640a:1160|1               |1                         |test.db_gb18030_test.tbl_test_1|1             |3          |1646129902884 |1002|A  |V2-1|90141|null|1646101923000|1646129902000|
|test     |db_gb18030_test|tbl_test_1|1646140400000|1             |mysql-bin.000019|61003   |u      |1646140673139|e45b718e-906f-11ec-89e3-0242c0a8640a:1192|2               |2                         |test.db_gb18030_test.tbl_test_1|2             |9          |1646140673712 |1003|A  |V3-1|3018 |null|1646101923000|1646140400000|
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+-----------------------------------------+----------------+--------------------------+-------------------------------+--------------+-----------+--------------+----+---+----+-----+----+-------------+-------------+

+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+-----------------------------------------+----------------+--------------------------+-------------------------------+--------------+-----------+--------------+----+---+----+----+----+----+-------------+-------------+
|_src_name|_src_db        |_src_table|_src_ts_ms   |_src_server_id|_src_file       |_src_pos|_src_op|_src_ts_ms_r |_tsc_id                                  |_tsc_total_order|_tsc_data_collection_order|_kfk_topic                     |_kfk_partition|_kfk_offset|_kfk_timestamp|id1 |id2|c1  |c2  |c4  |c3  |create_time  |update_time  |
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+-----------------------------------------+----------------+--------------------------+-------------------------------+--------------+-----------+--------------+----+---+----+----+----+----+-------------+-------------+
|test     |db_gb18030_test|tbl_test_1|1646295772000|1             |mysql-bin.000021|44808   |u      |1646295772102|e45b718e-906f-11ec-89e3-0242c0a8640a:1369|1               |1                         |test.db_gb18030_test.tbl_test_1|2             |12         |1646295772347 |1001|A  |V1-1|8012|2001|null|1646101923000|1646295772000|
|test     |db_gb18030_test|tbl_test_1|1646295926000|1             |mysql-bin.000021|45183   |u      |1646295926933|e45b718e-906f-11ec-89e3-0242c0a8640a:1370|1               |1                         |test.db_gb18030_test.tbl_test_1|2             |13         |1646295927284 |1003|A  |V3-1|3018|4020|null|1646101923000|1646295926000|
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+-----------------------------------------+----------------+--------------------------+-------------------------------+--------------+-----------+--------------+----+---+----+----+----+----+-------------+-------------+

+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+-----------------------------------------+----------------+--------------------------+-------------------------------+--------------+-----------+--------------+----+---+----+----+------+----+----+-------------+-------------+
|_src_name|_src_db        |_src_table|_src_ts_ms   |_src_server_id|_src_file       |_src_pos|_src_op|_src_ts_ms_r |_tsc_id                                  |_tsc_total_order|_tsc_data_collection_order|_kfk_topic                     |_kfk_partition|_kfk_offset|_kfk_timestamp|id1 |id2|c1  |c2  |c5    |c4  |c3  |create_time  |update_time  |
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+-----------------------------------------+----------------+--------------------------+-------------------------------+--------------+-----------+--------------+----+---+----+----+------+----+----+-------------+-------------+
|test     |db_gb18030_test|tbl_test_1|1646391849000|1             |mysql-bin.000021|128416  |u      |1646392447290|e45b718e-906f-11ec-89e3-0242c0a8640a:1419|1               |1                         |test.db_gb18030_test.tbl_test_1|0             |2          |1646392447848 |1005|A  |V3-1|5000|S4-421|4000|null|1646101923000|1646391849000|
|test     |db_gb18030_test|tbl_test_1|1646296999000|1             |mysql-bin.000021|46213   |c      |1646296999801|e45b718e-906f-11ec-89e3-0242c0a8640a:1373|1               |1                         |test.db_gb18030_test.tbl_test_1|2             |15         |1646297000283 |1004|A  |V3-1|4000|S4-1  |4000|null|1646101923000|1646296793000|
|test     |db_gb18030_test|tbl_test_1|1646297007000|1             |mysql-bin.000021|46573   |u      |1646297007714|e45b718e-906f-11ec-89e3-0242c0a8640a:1374|1               |1                         |test.db_gb18030_test.tbl_test_1|2             |16         |1646297007806 |1003|A  |V3-1|3019|null  |4021|null|1646101923000|1646297007000|
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+-----------------------------------------+----------------+--------------------------+-------------------------------+--------------+-----------+--------------+----+---+----+----+------+----+----+-------------+-------------+

+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+-----------------------------------------+----------------+--------------------------+-------------------------------+--------------+-----------+--------------+----+---+----+----+----+-----+----+----+-------------+-------------+
|_src_name|_src_db        |_src_table|_src_ts_ms   |_src_server_id|_src_file       |_src_pos|_src_op|_src_ts_ms_r |_tsc_id                                  |_tsc_total_order|_tsc_data_collection_order|_kfk_topic                     |_kfk_partition|_kfk_offset|_kfk_timestamp|id1 |id2|c1  |c2  |c6  |c5   |c4  |c3  |create_time  |update_time  |
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+-----------------------------------------+----------------+--------------------------+-------------------------------+--------------+-----------+--------------+----+---+----+----+----+-----+----+----+-------------+-------------+
|test     |db_gb18030_test|tbl_test_1|1646392418000|1             |mysql-bin.000021|129482  |u      |1646392447328|e45b718e-906f-11ec-89e3-0242c0a8640a:1422|1               |1                         |test.db_gb18030_test.tbl_test_1|0             |3          |1646392448005 |1005|A  |V3-1|5000|null|S4-44|4000|null|1646101923000|1646392418000|
|test     |db_gb18030_test|tbl_test_1|1646392418000|1             |mysql-bin.000021|129081  |u      |1646392447328|e45b718e-906f-11ec-89e3-0242c0a8640a:1421|1               |1                         |test.db_gb18030_test.tbl_test_1|2             |17         |1646392448004 |1004|A  |V3-1|4000|null|S4-14|4000|null|1646101923000|1646392418000|
+---------+---------------+----------+-------------+--------------+----------------+--------+-------+-------------+-----------------------------------------+----------------+--------------------------+-------------------------------+--------------+-----------+--------------+----+---+----+----+----+-----+----+----+-------------+-------------+
```

```text
[2022-03-04 21:50:04,765] INFO Registering signal handler for TERM (org.apache.spark.util.SignalUtils)
[2022-03-04 21:50:04,766] INFO Registering signal handler for HUP (org.apache.spark.util.SignalUtils)
[2022-03-04 21:50:04,766] INFO Registering signal handler for INT (org.apache.spark.util.SignalUtils)
[2022-03-04 21:50:05,053] INFO Changing view acls to: root (org.apache.spark.SecurityManager)
[2022-03-04 21:50:05,054] INFO Changing modify acls to: root (org.apache.spark.SecurityManager)
[2022-03-04 21:50:05,054] INFO Changing view acls groups to:  (org.apache.spark.SecurityManager)
[2022-03-04 21:50:05,054] INFO Changing modify acls groups to:  (org.apache.spark.SecurityManager)
[2022-03-04 21:50:05,054] INFO SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(root); groups with view permissions: Set(); users  with modify permissions: Set(root); groups with modify permissions: Set() (org.apache.spark.SecurityManager)
[2022-03-04 21:50:05,129] WARN Unable to load native-hadoop library for your platform... using builtin-java classes where applicable (org.apache.hadoop.util.NativeCodeLoader)
[2022-03-04 21:50:05,239] INFO ApplicationAttemptId: appattempt_1646396008293_0004_000001 (org.apache.spark.deploy.yarn.ApplicationMaster)
[2022-03-04 21:50:05,250] INFO Starting the user application in a separate Thread (org.apache.spark.deploy.yarn.ApplicationMaster)
[2022-03-04 21:50:05,273] INFO Waiting for spark context initialization... (org.apache.spark.deploy.yarn.ApplicationMaster)
[2022-03-04 21:50:05,276] INFO JobConfLoader load conf, confKey[mysql:HADOOP%G1] (org.apache.iceberg.streaming.config.JobCfgHelper)
[2022-03-04 21:50:05,423] INFO Found configuration file null (org.apache.hadoop.hive.conf.HiveConf)
[2022-03-04 21:50:05,538] INFO Running Spark version 3.2.1 (org.apache.spark.SparkContext)
[2022-03-04 21:50:05,555] INFO ============================================================== (org.apache.spark.resource.ResourceUtils)
[2022-03-04 21:50:05,555] INFO No custom resources configured for spark.driver. (org.apache.spark.resource.ResourceUtils)
[2022-03-04 21:50:05,555] INFO ============================================================== (org.apache.spark.resource.ResourceUtils)
[2022-03-04 21:50:05,555] INFO Submitted application: Kafka2Iceberg (org.apache.spark.SparkContext)
[2022-03-04 21:50:05,574] INFO Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0) (org.apache.spark.resource.ResourceProfile)
[2022-03-04 21:50:05,578] INFO Limiting resource is cpus at 1 tasks per executor (org.apache.spark.resource.ResourceProfile)
[2022-03-04 21:50:05,580] INFO Added ResourceProfile id: 0 (org.apache.spark.resource.ResourceProfileManager)
[2022-03-04 21:50:05,611] INFO Changing view acls to: root (org.apache.spark.SecurityManager)
[2022-03-04 21:50:05,611] INFO Changing modify acls to: root (org.apache.spark.SecurityManager)
[2022-03-04 21:50:05,611] INFO Changing view acls groups to:  (org.apache.spark.SecurityManager)
[2022-03-04 21:50:05,611] INFO Changing modify acls groups to:  (org.apache.spark.SecurityManager)
[2022-03-04 21:50:05,611] INFO SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(root); groups with view permissions: Set(); users  with modify permissions: Set(root); groups with modify permissions: Set() (org.apache.spark.SecurityManager)
[2022-03-04 21:50:05,745] INFO Successfully started service 'sparkDriver' on port 36241. (org.apache.spark.util.Utils)
[2022-03-04 21:50:05,762] INFO Registering MapOutputTracker (org.apache.spark.SparkEnv)
[2022-03-04 21:50:05,782] INFO Registering BlockManagerMaster (org.apache.spark.SparkEnv)
[2022-03-04 21:50:05,793] INFO Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information (org.apache.spark.storage.BlockManagerMasterEndpoint)
[2022-03-04 21:50:05,793] INFO BlockManagerMasterEndpoint up (org.apache.spark.storage.BlockManagerMasterEndpoint)
[2022-03-04 21:50:05,814] INFO Registering BlockManagerMasterHeartbeat (org.apache.spark.SparkEnv)
[2022-03-04 21:50:05,854] INFO Created local directory at /opt/installs/hadoop-2.7.5/data/tmp/nm-local-dir/usercache/root/appcache/application_1646396008293_0004/blockmgr-d33e0618-2f14-4282-8e7e-22e80d76dbb4 (org.apache.spark.storage.DiskBlockManager)
[2022-03-04 21:50:05,868] INFO MemoryStore started with capacity 366.3 MiB (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:05,897] INFO Registering OutputCommitCoordinator (org.apache.spark.SparkEnv)
[2022-03-04 21:50:05,956] INFO Logging initialized @1667ms to org.sparkproject.jetty.util.log.Slf4jLog (org.sparkproject.jetty.util.log)
[2022-03-04 21:50:06,000] INFO jetty-9.4.43.v20210629; built: 2021-06-30T11:07:22.254Z; git: 526006ecfa3af7f1a27ef3a288e2bef7ea9dd7e8; jvm 1.8.0_312-b07 (org.sparkproject.jetty.server.Server)
[2022-03-04 21:50:06,014] INFO Started @1726ms (org.sparkproject.jetty.server.Server)
[2022-03-04 21:50:06,038] INFO Started ServerConnector@8eed236{HTTP/1.1, (http/1.1)}{0.0.0.0:34641} (org.sparkproject.jetty.server.AbstractConnector)
[2022-03-04 21:50:06,038] INFO Successfully started service 'SparkUI' on port 34641. (org.apache.spark.util.Utils)
[2022-03-04 21:50:06,039] INFO Adding filter to /jobs: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,059] INFO Started o.s.j.s.ServletContextHandler@10a4a93c{/jobs,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,060] INFO Adding filter to /jobs/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,061] INFO Started o.s.j.s.ServletContextHandler@518d1038{/jobs/json,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,061] INFO Adding filter to /jobs/job: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,062] INFO Started o.s.j.s.ServletContextHandler@518ffa83{/jobs/job,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,062] INFO Adding filter to /jobs/job/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,064] INFO Started o.s.j.s.ServletContextHandler@52a731d6{/jobs/job/json,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,064] INFO Adding filter to /stages: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,065] INFO Started o.s.j.s.ServletContextHandler@4fb9487{/stages,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,065] INFO Adding filter to /stages/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,065] INFO Started o.s.j.s.ServletContextHandler@1c67f8af{/stages/json,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,066] INFO Adding filter to /stages/stage: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,066] INFO Started o.s.j.s.ServletContextHandler@3b106e46{/stages/stage,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,066] INFO Adding filter to /stages/stage/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,067] INFO Started o.s.j.s.ServletContextHandler@781b5c24{/stages/stage/json,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,067] INFO Adding filter to /stages/pool: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,068] INFO Started o.s.j.s.ServletContextHandler@6418cadd{/stages/pool,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,068] INFO Adding filter to /stages/pool/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,068] INFO Started o.s.j.s.ServletContextHandler@4408ed47{/stages/pool/json,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,069] INFO Adding filter to /storage: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,069] INFO Started o.s.j.s.ServletContextHandler@5124ef12{/storage,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,069] INFO Adding filter to /storage/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,069] INFO Started o.s.j.s.ServletContextHandler@3741472f{/storage/json,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,070] INFO Adding filter to /storage/rdd: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,070] INFO Started o.s.j.s.ServletContextHandler@7c8845e9{/storage/rdd,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,070] INFO Adding filter to /storage/rdd/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,071] INFO Started o.s.j.s.ServletContextHandler@788b132d{/storage/rdd/json,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,071] INFO Adding filter to /environment: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,071] INFO Started o.s.j.s.ServletContextHandler@7e6b3e14{/environment,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,072] INFO Adding filter to /environment/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,073] INFO Started o.s.j.s.ServletContextHandler@c1b895d{/environment/json,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,073] INFO Adding filter to /executors: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,073] INFO Started o.s.j.s.ServletContextHandler@1273eab3{/executors,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,074] INFO Adding filter to /executors/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,074] INFO Started o.s.j.s.ServletContextHandler@1b51f004{/executors/json,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,074] INFO Adding filter to /executors/threadDump: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,075] INFO Started o.s.j.s.ServletContextHandler@1ec56e8b{/executors/threadDump,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,075] INFO Adding filter to /executors/threadDump/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,075] INFO Started o.s.j.s.ServletContextHandler@6574b04b{/executors/threadDump/json,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,075] INFO Adding filter to /static: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,082] INFO Started o.s.j.s.ServletContextHandler@37daeecc{/static,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,082] INFO Adding filter to /: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,083] INFO Started o.s.j.s.ServletContextHandler@21fd59cb{/,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,083] INFO Adding filter to /api: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,084] INFO Started o.s.j.s.ServletContextHandler@4aebd28a{/api,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,084] INFO Adding filter to /jobs/job/kill: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,085] INFO Started o.s.j.s.ServletContextHandler@6f4ecb9{/jobs/job/kill,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,085] INFO Adding filter to /stages/stage/kill: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,085] INFO Started o.s.j.s.ServletContextHandler@4c07b7df{/stages/stage/kill,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,086] INFO Bound SparkUI to 0.0.0.0, and started at http://hadoop:34641 (org.apache.spark.ui.SparkUI)
[2022-03-04 21:50:06,140] INFO Created YarnClusterScheduler (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:50:06,208] INFO Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 40849. (org.apache.spark.util.Utils)
[2022-03-04 21:50:06,208] INFO Server created on hadoop:40849 (org.apache.spark.network.netty.NettyBlockTransferService)
[2022-03-04 21:50:06,209] INFO Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy (org.apache.spark.storage.BlockManager)
[2022-03-04 21:50:06,214] INFO Registering BlockManager BlockManagerId(driver, hadoop, 40849, None) (org.apache.spark.storage.BlockManagerMaster)
[2022-03-04 21:50:06,216] INFO Registering block manager hadoop:40849 with 366.3 MiB RAM, BlockManagerId(driver, hadoop, 40849, None) (org.apache.spark.storage.BlockManagerMasterEndpoint)
[2022-03-04 21:50:06,217] INFO Registered BlockManager BlockManagerId(driver, hadoop, 40849, None) (org.apache.spark.storage.BlockManagerMaster)
[2022-03-04 21:50:06,218] INFO Initialized BlockManager: BlockManagerId(driver, hadoop, 40849, None) (org.apache.spark.storage.BlockManager)
[2022-03-04 21:50:06,333] INFO Adding filter to /metrics/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:06,334] INFO Started o.s.j.s.ServletContextHandler@5d46641b{/metrics/json,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:06,392] INFO Connecting to ResourceManager at hadoop/192.168.100.60:8030 (org.apache.hadoop.yarn.client.RMProxy)
[2022-03-04 21:50:06,417] INFO Registering the ApplicationMaster (org.apache.spark.deploy.yarn.YarnRMClient)
[2022-03-04 21:50:06,488] INFO Preparing Local resources (org.apache.spark.deploy.yarn.ApplicationMaster)
[2022-03-04 21:50:06,921] INFO 
===============================================================================
Default YARN executor launch context:
  env:
    CLASSPATH -> {{PWD}}<CPS>{{PWD}}/__spark_conf__<CPS>{{PWD}}/__spark_libs__/*<CPS>{{PWD}}/__spark_conf__/__hadoop_conf__
    SPARK_YARN_STAGING_DIR -> hdfs://hadoop:8020/user/root/.sparkStaging/application_1646396008293_0004
    SPARK_USER -> root

  command:
    {{JAVA_HOME}}/bin/java \ 
      -server \ 
      -Xmx1024m \ 
      '-Dlog4j.configuration=log4j-executor.properties' \ 
      '-Dvm.logging.level=INFO' \ 
      '-Dvm.logging.name=kafka2iceberg-1' \ 
      -Djava.io.tmpdir={{PWD}}/tmp \ 
      '-Dspark.driver.port=36241' \ 
      '-Dspark.ui.port=0' \ 
      -Dspark.yarn.app.container.log.dir=<LOG_DIR> \ 
      -XX:OnOutOfMemoryError='kill %p' \ 
      org.apache.spark.executor.YarnCoarseGrainedExecutorBackend \ 
      --driver-url \ 
      spark://CoarseGrainedScheduler@hadoop:36241 \ 
      --executor-id \ 
      <executorId> \ 
      --hostname \ 
      <hostname> \ 
      --cores \ 
      1 \ 
      --app-id \ 
      application_1646396008293_0004 \ 
      --resourceProfileId \ 
      0 \ 
      --user-class-path \ 
      file:$PWD/__app__.jar \ 
      1><LOG_DIR>/stdout \ 
      2><LOG_DIR>/stderr

  resources:
    __spark_libs__/spire-platform_2.12-0.17.0.jar -> resource { scheme: "hdfs" host: "hadoop" port: 8020 file: "/user/share/libs/spark/3.2.1/spire-platform_2.12-0.17.0.jar" } size: 8455 timestamp: 1646100993177 type: FILE visibility: PUBLIC
     __spark_libs__/hk2-api-2.6.1.jar -> resource { scheme: "hdfs" host: "hadoop" port: 8020 file: "/user/share/libs/spark/3.2.1/hk2-api-2.6.1.jar" } size: 200223 timestamp: 1646100952089 type: FILE visibility: PUBLIC

=============================================================================== (org.apache.spark.deploy.yarn.ApplicationMaster)
[2022-03-04 21:50:06,940] INFO Resource profile 0 doesn't exist, adding it (org.apache.spark.deploy.yarn.YarnAllocator)
[2022-03-04 21:50:06,945] INFO ApplicationMaster registered as NettyRpcEndpointRef(spark://YarnAM@hadoop:36241) (org.apache.spark.scheduler.cluster.YarnSchedulerBackend$YarnSchedulerEndpoint)
[2022-03-04 21:50:06,949] INFO Will request 2 executor container(s) for  ResourceProfile Id: 0, each with 1 core(s) and 1408 MB memory. (org.apache.spark.deploy.yarn.YarnAllocator)
[2022-03-04 21:50:06,957] INFO Submitted 2 unlocalized container requests. (org.apache.spark.deploy.yarn.YarnAllocator)
[2022-03-04 21:50:07,038] INFO Started progress reporter thread with (heartbeat : 3000, initial allocation : 200) intervals (org.apache.spark.deploy.yarn.ApplicationMaster)
[2022-03-04 21:50:07,656] INFO Received new token for : hadoop:46205 (org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl)
[2022-03-04 21:50:07,662] INFO Launching container container_1646396008293_0004_01_000002 on host hadoop for executor with ID 1 for ResourceProfile Id 0 (org.apache.spark.deploy.yarn.YarnAllocator)
[2022-03-04 21:50:07,664] INFO Received 1 containers from YARN, launching executors on 1 of them. (org.apache.spark.deploy.yarn.YarnAllocator)
[2022-03-04 21:50:07,667] INFO yarn.client.max-cached-nodemanagers-proxies : 0 (org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy)
[2022-03-04 21:50:07,681] INFO Opening proxy : hadoop:46205 (org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy)
[2022-03-04 21:50:09,690] INFO Registered executor NettyRpcEndpointRef(spark-client://Executor) (192.168.100.60:49536) with ID 1,  ResourceProfileId 0 (org.apache.spark.scheduler.cluster.YarnSchedulerBackend$YarnDriverEndpoint)
[2022-03-04 21:50:09,759] INFO Registering block manager hadoop:46193 with 366.3 MiB RAM, BlockManagerId(1, hadoop, 46193, None) (org.apache.spark.storage.BlockManagerMasterEndpoint)
[2022-03-04 21:50:36,186] INFO SchedulerBackend is ready for scheduling beginning after waiting maxRegisteredResourcesWaitingTime: 30000000000(ns) (org.apache.spark.scheduler.cluster.YarnClusterSchedulerBackend)
[2022-03-04 21:50:36,187] INFO YarnClusterScheduler.postStartHook done (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:50:36,261] INFO Table Configure [spark.sql.warehouse.dir -> hdfs://hadoop:8020/user/hive/warehouse, iceberg.table.partitionBy -> id1,id2, kafka.consumer.topic -> test.db_gb18030_test.tbl_test_1, iceberg.table.comment -> db_gb18030_test tbl_test_1, spark.hadoop.hive.metastore.uris -> thrift://hadoop:9083, hive.jdbc.url -> jdbc:hive2://hadoop:10000, iceberg.table.properties -> 'read.split.target-size'='268435456','write.metadata.delete-after-commit.enabled'='true', record.metadata.source.columns -> name, db, table, ts_ms, server_id, file, pos, kafka.consumer.max.poll.records -> 5, spark.sql.extensions -> org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions, iceberg.table.name -> hadoop.db_gb18030_test.tbl_test_1, kafka.bootstrap.servers -> kafka:9092, record.metadata.source.prefix -> _src_, record.metadata.kafka.columns -> topic, partition, offset, timestamp, kafka.consumer.commit.timeout.millis -> 60000, spark.sql.sources.partitionOverwriteMode -> dynamic, hive.jdbc.password -> , kafka.consumer.group.id -> iceberg-hadoop, hive.jdbc.user -> , spark.streaming.kafka.maxRatePerPartition -> 10000, kafka.consumer.key.deserializer -> io.confluent.kafka.serializers.KafkaAvroDeserializer, spark.sql.catalog.hadoop.type -> hadoop, record.metadata.transaction.prefix -> _tsc_, spark.yarn.jars -> hdfs:/user/share/libs/spark/3.2.1/*.jar,hdfs:/user/share/libs/kafka2iceberg/0.1.0/*.jar, record.metadata.kafka.prefix -> _kfk_, kafka.schema.registry.url -> http://kafka:8081, spark.master -> yarn, hive.external.jar -> hdfs://hadoop:8020/user/share/libs/iceberg-runtime/0.13.1/iceberg-hive-runtime-0.13.1.jar, spark.sql.catalog.hadoop -> org.apache.iceberg.spark.SparkCatalog, kafka.consumer.value.deserializer -> io.confluent.kafka.serializers.KafkaAvroDeserializer, iceberg.table.primaryKey -> id1,id2, spark.app.name -> Kafka2Iceberg, spark.sql.catalog.hadoop.warehouse -> hdfs://hadoop:8020/user/test/iceberg, record.metadata.transaction.columns -> id, total_order, data_collection_order] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:50:36,264] INFO Get next batch min schema version by kafka commit offset, because StatusAccumulator status is null (org.apache.iceberg.streaming.utils.SchemaUtils)
[2022-03-04 21:50:36,280] INFO ConsumerConfig values: 

 (io.confluent.kafka.serializers.KafkaAvroDeserializerConfig)
[2022-03-04 21:50:36,343] INFO Kafka version: 2.8.0 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:36,343] INFO Kafka commitId: ebb1d6e21cc92130 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:36,343] INFO Kafka startTimeMs: 1646401836342 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:36,532] INFO [Consumer clientId=consumer-iceberg-hadoop-1, groupId=iceberg-hadoop] Cluster ID: z9nnmK3URTSygqYnuIgGJg (org.apache.kafka.clients.Metadata)
[2022-03-04 21:50:36,544] INFO [Consumer clientId=consumer-iceberg-hadoop-1, groupId=iceberg-hadoop] Discovered group coordinator 192.168.100.30:9092 (id: 2147483647 rack: null) (org.apache.kafka.clients.consumer.internals.AbstractCoordinator)
[2022-03-04 21:50:36,547] INFO [Consumer clientId=consumer-iceberg-hadoop-1, groupId=iceberg-hadoop] Found no committed offset for partition test.db_gb18030_test.tbl_test_1-0 (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:36,547] INFO [Consumer clientId=consumer-iceberg-hadoop-1, groupId=iceberg-hadoop] Found no committed offset for partition test.db_gb18030_test.tbl_test_1-1 (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:36,547] INFO [Consumer clientId=consumer-iceberg-hadoop-1, groupId=iceberg-hadoop] Found no committed offset for partition test.db_gb18030_test.tbl_test_1-2 (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:36,549] INFO [Consumer clientId=consumer-iceberg-hadoop-1, groupId=iceberg-hadoop] Subscribed to partition(s): test.db_gb18030_test.tbl_test_1-0 (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:36,551] INFO [Consumer clientId=consumer-iceberg-hadoop-1, groupId=iceberg-hadoop] Seeking to offset 0 for partition test.db_gb18030_test.tbl_test_1-0 (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:36,552] INFO load partition [test.db_gb18030_test.tbl_test_1-0] schema info from offset [0] (org.apache.iceberg.streaming.utils.SchemaUtils)
[2022-03-04 21:50:36,747] INFO [Consumer clientId=consumer-iceberg-hadoop-1, groupId=iceberg-hadoop] Unsubscribed all topics or patterns and assigned partitions (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:36,748] INFO [Consumer clientId=consumer-iceberg-hadoop-1, groupId=iceberg-hadoop] Subscribed to partition(s): test.db_gb18030_test.tbl_test_1-1 (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:36,748] INFO [Consumer clientId=consumer-iceberg-hadoop-1, groupId=iceberg-hadoop] Seeking to offset 0 for partition test.db_gb18030_test.tbl_test_1-1 (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:36,748] INFO load partition [test.db_gb18030_test.tbl_test_1-1] schema info from offset [0] (org.apache.iceberg.streaming.utils.SchemaUtils)
[2022-03-04 21:50:37,248] INFO [Consumer clientId=consumer-iceberg-hadoop-1, groupId=iceberg-hadoop] Unsubscribed all topics or patterns and assigned partitions (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:37,248] INFO [Consumer clientId=consumer-iceberg-hadoop-1, groupId=iceberg-hadoop] Subscribed to partition(s): test.db_gb18030_test.tbl_test_1-2 (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:37,248] INFO [Consumer clientId=consumer-iceberg-hadoop-1, groupId=iceberg-hadoop] Seeking to offset 0 for partition test.db_gb18030_test.tbl_test_1-2 (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:37,248] INFO load partition [test.db_gb18030_test.tbl_test_1-2] schema info from offset [0] (org.apache.iceberg.streaming.utils.SchemaUtils)
[2022-03-04 21:50:37,759] INFO [Consumer clientId=consumer-iceberg-hadoop-1, groupId=iceberg-hadoop] Unsubscribed all topics or patterns and assigned partitions (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:37,759] INFO Metrics scheduler closed (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:37,760] INFO Closing reporter org.apache.kafka.common.metrics.JmxReporter (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:37,760] INFO Metrics reporters closed (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:37,764] INFO App info kafka.consumer for consumer-iceberg-hadoop-1 unregistered (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:37,765] INFO Kafka partition schema version [test.db_gb18030_test.tbl_test_1-1 -> 1,test.db_gb18030_test.tbl_test_1-0 -> 4,test.db_gb18030_test.tbl_test_1-2 -> 1] (org.apache.iceberg.streaming.utils.SchemaUtils)
[2022-03-04 21:50:37,766] INFO All partition current offset equal with begging offset, set next bach schema version to min version  (org.apache.iceberg.streaming.utils.SchemaUtils)
[2022-03-04 21:50:37,775] INFO ConsumerConfig values: 

 (io.confluent.kafka.serializers.KafkaAvroDeserializerConfig)
[2022-03-04 21:50:37,780] INFO Kafka version: 2.8.0 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:37,780] INFO Kafka commitId: ebb1d6e21cc92130 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:37,780] INFO Kafka startTimeMs: 1646401837779 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:37,784] INFO [Consumer clientId=consumer-iceberg-hadoop-2, groupId=iceberg-hadoop] Cluster ID: z9nnmK3URTSygqYnuIgGJg (org.apache.kafka.clients.Metadata)
[2022-03-04 21:50:37,787] INFO [Consumer clientId=consumer-iceberg-hadoop-2, groupId=iceberg-hadoop] Discovered group coordinator 192.168.100.30:9092 (id: 2147483647 rack: null) (org.apache.kafka.clients.consumer.internals.AbstractCoordinator)
[2022-03-04 21:50:37,790] INFO [Consumer clientId=consumer-iceberg-hadoop-2, groupId=iceberg-hadoop] Found no committed offset for partition test.db_gb18030_test.tbl_test_1-0 (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:37,790] INFO [Consumer clientId=consumer-iceberg-hadoop-2, groupId=iceberg-hadoop] Found no committed offset for partition test.db_gb18030_test.tbl_test_1-1 (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:37,790] INFO [Consumer clientId=consumer-iceberg-hadoop-2, groupId=iceberg-hadoop] Found no committed offset for partition test.db_gb18030_test.tbl_test_1-2 (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:37,793] INFO Metrics scheduler closed (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:37,793] INFO Closing reporter org.apache.kafka.common.metrics.JmxReporter (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:37,793] INFO Metrics reporters closed (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:37,794] INFO App info kafka.consumer for consumer-iceberg-hadoop-2 unregistered (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:37,869] INFO Block broadcast_0 stored as values in memory (estimated size 245.6 KiB, free 366.1 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:37,902] INFO Block broadcast_0_piece0 stored as bytes in memory (estimated size 1460.0 B, free 366.1 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:37,914] INFO Added broadcast_0_piece0 in memory on hadoop:40849 (size: 1460.0 B, free: 366.3 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:37,920] INFO Created broadcast 0 from broadcast at Kafka2Iceberg.scala:288 (org.apache.spark.SparkContext)
[2022-03-04 21:50:37,921] INFO After Init StatusAccumulator [
Map(
    hadoop.db_gb18030_test.tbl_test_1 -> 
        StatusAccumulator(
            partitionOffsets: Map(
                test.db_gb18030_test.tbl_test_1:0 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 0, range: [0 -> 0], curOffset: 0),
                test.db_gb18030_test.tbl_test_1:1 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 1, range: [0 -> 0], curOffset: 0),
                test.db_gb18030_test.tbl_test_1:2 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 2, range: [0 -> 0], curOffset: 0)
                ),
            schemaVersion: 1
        )
)] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:50:37,930] INFO After Init SchemaBroadcast   [
Map(
    hadoop.db_gb18030_test.tbl_test_1 -> 
        SchemaBroadcast(
            schemaToVersionMap: Map(
                {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"C3","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"} -> 2,
                {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"} -> 1,
                {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"C4","type":["null","int"],"default":null},{"name":"C3","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"} -> 4,
                {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"C6","type":["null","long"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"C4","type":["null","int"],"default":null},{"name":"C3","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"} -> 5,
                {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"C4","type":["null","int"],"default":null},{"name":"C3","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"} -> 3),
            versionToSchemaMap: Map(
                5 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"C6","type":["null","long"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"C4","type":["null","int"],"default":null},{"name":"C3","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"},
                1 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"},
                2 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"C3","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"},
                3 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"C4","type":["null","int"],"default":null},{"name":"C3","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"},
                4 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"C4","type":["null","int"],"default":null},{"name":"C3","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"})
            )
)] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:50:38,040] WARN overriding enable.auto.commit to false for executor (org.apache.spark.streaming.kafka010.KafkaUtils)
[2022-03-04 21:50:38,040] WARN overriding auto.offset.reset to none for executor (org.apache.spark.streaming.kafka010.KafkaUtils)
[2022-03-04 21:50:38,040] WARN overriding executor group.id to spark-executor-iceberg-hadoop (org.apache.spark.streaming.kafka010.KafkaUtils)
[2022-03-04 21:50:38,040] WARN overriding receive.buffer.bytes to 65536 see KAFKA-3135 (org.apache.spark.streaming.kafka010.KafkaUtils)
[2022-03-04 21:50:38,077] INFO Slide time = 20000 ms (org.apache.spark.streaming.kafka010.DirectKafkaInputDStream)
[2022-03-04 21:50:38,078] INFO Storage level = Serialized 1x Replicated (org.apache.spark.streaming.kafka010.DirectKafkaInputDStream)
[2022-03-04 21:50:38,078] INFO Checkpoint interval = null (org.apache.spark.streaming.kafka010.DirectKafkaInputDStream)
[2022-03-04 21:50:38,078] INFO Remember interval = 20000 ms (org.apache.spark.streaming.kafka010.DirectKafkaInputDStream)
[2022-03-04 21:50:38,078] INFO Initialized and validated org.apache.spark.streaming.kafka010.DirectKafkaInputDStream@4660bdf6 (org.apache.spark.streaming.kafka010.DirectKafkaInputDStream)
[2022-03-04 21:50:38,078] INFO Slide time = 20000 ms (org.apache.spark.streaming.dstream.ForEachDStream)
[2022-03-04 21:50:38,078] INFO Storage level = Serialized 1x Replicated (org.apache.spark.streaming.dstream.ForEachDStream)
[2022-03-04 21:50:38,078] INFO Checkpoint interval = null (org.apache.spark.streaming.dstream.ForEachDStream)
[2022-03-04 21:50:38,078] INFO Remember interval = 20000 ms (org.apache.spark.streaming.dstream.ForEachDStream)
[2022-03-04 21:50:38,078] INFO Initialized and validated org.apache.spark.streaming.dstream.ForEachDStream@497c870d (org.apache.spark.streaming.dstream.ForEachDStream)
[2022-03-04 21:50:38,108] INFO ConsumerConfig values: 

 (io.confluent.kafka.serializers.KafkaAvroDeserializerConfig)
[2022-03-04 21:50:38,111] INFO Kafka version: 2.8.0 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:38,111] INFO Kafka commitId: ebb1d6e21cc92130 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:38,111] INFO Kafka startTimeMs: 1646401838111 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:38,113] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Subscribed to topic(s): test.db_gb18030_test.tbl_test_1 (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:38,116] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Cluster ID: z9nnmK3URTSygqYnuIgGJg (org.apache.kafka.clients.Metadata)
[2022-03-04 21:50:38,116] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Discovered group coordinator 192.168.100.30:9092 (id: 2147483647 rack: null) (org.apache.kafka.clients.consumer.internals.AbstractCoordinator)
[2022-03-04 21:50:38,119] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] (Re-)joining group (org.apache.kafka.clients.consumer.internals.AbstractCoordinator)
[2022-03-04 21:50:38,126] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] (Re-)joining group (org.apache.kafka.clients.consumer.internals.AbstractCoordinator)
[2022-03-04 21:50:38,128] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Successfully joined group with generation Generation{generationId=9, memberId='consumer-iceberg-hadoop-3-79a4d99f-ec82-4732-818d-953ffbc7eb22', protocol='range'} (org.apache.kafka.clients.consumer.internals.AbstractCoordinator)
[2022-03-04 21:50:38,129] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Finished assignment for group at generation 9: {consumer-iceberg-hadoop-3-79a4d99f-ec82-4732-818d-953ffbc7eb22=Assignment(partitions=[test.db_gb18030_test.tbl_test_1-0, test.db_gb18030_test.tbl_test_1-1, test.db_gb18030_test.tbl_test_1-2])} (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:38,133] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Successfully synced group in generation Generation{generationId=9, memberId='consumer-iceberg-hadoop-3-79a4d99f-ec82-4732-818d-953ffbc7eb22', protocol='range'} (org.apache.kafka.clients.consumer.internals.AbstractCoordinator)
[2022-03-04 21:50:38,133] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Notifying assignor about the new Assignment(partitions=[test.db_gb18030_test.tbl_test_1-0, test.db_gb18030_test.tbl_test_1-1, test.db_gb18030_test.tbl_test_1-2]) (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:38,133] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Adding newly assigned partitions: test.db_gb18030_test.tbl_test_1-0, test.db_gb18030_test.tbl_test_1-1, test.db_gb18030_test.tbl_test_1-2 (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:38,134] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Found no committed offset for partition test.db_gb18030_test.tbl_test_1-0 (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:38,134] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Found no committed offset for partition test.db_gb18030_test.tbl_test_1-1 (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:38,134] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Found no committed offset for partition test.db_gb18030_test.tbl_test_1-2 (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:38,138] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Resetting offset for partition test.db_gb18030_test.tbl_test_1-0 to position FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}}. (org.apache.kafka.clients.consumer.internals.SubscriptionState)
[2022-03-04 21:50:38,138] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Resetting offset for partition test.db_gb18030_test.tbl_test_1-1 to position FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}}. (org.apache.kafka.clients.consumer.internals.SubscriptionState)
[2022-03-04 21:50:38,138] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Resetting offset for partition test.db_gb18030_test.tbl_test_1-2 to position FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}}. (org.apache.kafka.clients.consumer.internals.SubscriptionState)
[2022-03-04 21:50:38,139] INFO Started timer for JobGenerator at time 1646401840000 (org.apache.spark.streaming.util.RecurringTimer)
[2022-03-04 21:50:38,139] INFO Started JobGenerator at 1646401840000 ms (org.apache.spark.streaming.scheduler.JobGenerator)
[2022-03-04 21:50:38,140] INFO Started JobScheduler (org.apache.spark.streaming.scheduler.JobScheduler)
[2022-03-04 21:50:38,142] INFO Adding filter to /streaming: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:38,143] INFO Started o.s.j.s.ServletContextHandler@3a029522{/streaming,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:38,143] INFO Adding filter to /streaming/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:38,143] INFO Started o.s.j.s.ServletContextHandler@32891eed{/streaming/json,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:38,143] INFO Adding filter to /streaming/batch: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:38,144] INFO Started o.s.j.s.ServletContextHandler@4f7a19c3{/streaming/batch,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:38,144] INFO Adding filter to /streaming/batch/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:38,144] INFO Started o.s.j.s.ServletContextHandler@2585635a{/streaming/batch/json,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:38,145] INFO Adding filter to /static/streaming: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:38,145] INFO Started o.s.j.s.ServletContextHandler@6e223945{/static/streaming,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:38,145] INFO StreamingContext started (org.apache.spark.streaming.StreamingContext)
[2022-03-04 21:50:40,024] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Seeking to LATEST offset of partition test.db_gb18030_test.tbl_test_1-0 (org.apache.kafka.clients.consumer.internals.SubscriptionState)
[2022-03-04 21:50:40,024] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Seeking to LATEST offset of partition test.db_gb18030_test.tbl_test_1-2 (org.apache.kafka.clients.consumer.internals.SubscriptionState)
[2022-03-04 21:50:40,025] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Seeking to LATEST offset of partition test.db_gb18030_test.tbl_test_1-1 (org.apache.kafka.clients.consumer.internals.SubscriptionState)
[2022-03-04 21:50:40,029] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Resetting offset for partition test.db_gb18030_test.tbl_test_1-0 to position FetchPosition{offset=4, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}}. (org.apache.kafka.clients.consumer.internals.SubscriptionState)
[2022-03-04 21:50:40,029] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Resetting offset for partition test.db_gb18030_test.tbl_test_1-1 to position FetchPosition{offset=4, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}}. (org.apache.kafka.clients.consumer.internals.SubscriptionState)
[2022-03-04 21:50:40,029] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Resetting offset for partition test.db_gb18030_test.tbl_test_1-2 to position FetchPosition{offset=18, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}}. (org.apache.kafka.clients.consumer.internals.SubscriptionState)
[2022-03-04 21:50:40,051] INFO Added jobs for time 1646401840000 ms (org.apache.spark.streaming.scheduler.JobScheduler)
[2022-03-04 21:50:40,053] INFO Starting job streaming job 1646401840000 ms.0 from job set of time 1646401840000 ms (org.apache.spark.streaming.scheduler.JobScheduler)
[2022-03-04 21:50:40,054] INFO ---------------------------------------------------------------------------------- (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:50:40,055] INFO before write accumulator value [
        StatusAccumulator(
            partitionOffsets: Map(
                test.db_gb18030_test.tbl_test_1:0 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 0, range: [0 -> 4], curOffset: 0),
                test.db_gb18030_test.tbl_test_1:1 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 1, range: [0 -> 4], curOffset: 0),
                test.db_gb18030_test.tbl_test_1:2 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 2, range: [0 -> 18], curOffset: 0)
                ),
            schemaVersion: 1
        )
] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:50:40,055] INFO current offset ranges: [OffsetRange(topic: 'test.db_gb18030_test.tbl_test_1', partition: 0, range: [0 -> 4]),OffsetRange(topic: 'test.db_gb18030_test.tbl_test_1', partition: 1, range: [0 -> 4]),OffsetRange(topic: 'test.db_gb18030_test.tbl_test_1', partition: 2, range: [0 -> 18])] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:50:40,055] INFO current partition offsets: [test.db_gb18030_test.tbl_test_1:0 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 0, range: [0 -> 4], curOffset: 0),test.db_gb18030_test.tbl_test_1:1 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 1, range: [0 -> 4], curOffset: 0),test.db_gb18030_test.tbl_test_1:2 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 2, range: [0 -> 18], curOffset: 0)] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:50:40,059] INFO beginning write data into hadoop.db_gb18030_test.tbl_test_1 ... (org.apache.iceberg.streaming.write.IcebergWriter)
[2022-03-04 21:50:40,153] INFO Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir. (org.apache.spark.sql.internal.SharedState)
[2022-03-04 21:50:40,155] INFO Warehouse path is 'hdfs://hadoop:8020/user/hive/warehouse'. (org.apache.spark.sql.internal.SharedState)
[2022-03-04 21:50:40,166] INFO Adding filter to /SQL: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:40,180] INFO Started o.s.j.s.ServletContextHandler@5f45dcb6{/SQL,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:40,180] INFO Adding filter to /SQL/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:40,181] INFO Started o.s.j.s.ServletContextHandler@41366ed5{/SQL/json,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:40,181] INFO Adding filter to /SQL/execution: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:40,182] INFO Started o.s.j.s.ServletContextHandler@5f3d4259{/SQL/execution,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:40,182] INFO Adding filter to /SQL/execution/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:40,182] INFO Started o.s.j.s.ServletContextHandler@6706458a{/SQL/execution/json,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:40,183] INFO Adding filter to /static/sql: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:40,183] INFO Started o.s.j.s.ServletContextHandler@2c0f4fe5{/static/sql,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:42,228] INFO Code generated in 172.451583 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:50:42,244] INFO Starting job: show at IcebergWriter.scala:117 (org.apache.spark.SparkContext)
[2022-03-04 21:50:42,255] INFO Got job 0 (show at IcebergWriter.scala:117) with 1 output partitions (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:42,256] INFO Final stage: ResultStage 0 (show at IcebergWriter.scala:117) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:42,256] INFO Parents of final stage: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:42,256] INFO Missing parents: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:42,260] INFO Submitting ResultStage 0 (MapPartitionsRDD[4] at show at IcebergWriter.scala:117), which has no missing parents (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:42,360] INFO Block broadcast_1 stored as values in memory (estimated size 51.9 KiB, free 366.0 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:42,362] INFO Block broadcast_1_piece0 stored as bytes in memory (estimated size 15.2 KiB, free 366.0 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:42,363] INFO Added broadcast_1_piece0 in memory on hadoop:40849 (size: 15.2 KiB, free: 366.3 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:42,363] INFO Created broadcast 1 from broadcast at DAGScheduler.scala:1478 (org.apache.spark.SparkContext)
[2022-03-04 21:50:42,375] INFO Submitting 1 missing tasks from ResultStage 0 (MapPartitionsRDD[4] at show at IcebergWriter.scala:117) (first 15 tasks are for partitions Vector(0)) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:42,375] INFO Adding task set 0.0 with 1 tasks resource profile 0 (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:50:42,404] INFO Starting task 0.0 in stage 0.0 (TID 0) (hadoop, executor 1, partition 0, PROCESS_LOCAL, 4368 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:42,538] INFO Added broadcast_1_piece0 in memory on hadoop:46193 (size: 15.2 KiB, free: 366.3 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:43,712] INFO Finished task 0.0 in stage 0.0 (TID 0) in 1314 ms on hadoop (executor 1) (1/1) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:43,713] INFO Removed TaskSet 0.0, whose tasks have all completed, from pool  (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:50:43,718] INFO ResultStage 0 (show at IcebergWriter.scala:117) finished in 1.440 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:43,720] INFO Job 0 is finished. Cancelling potential speculative or zombie tasks for this job (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:43,721] INFO Killing all running tasks in stage 0: Stage finished (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:50:43,722] INFO Job 0 finished: show at IcebergWriter.scala:117, took 1.477943 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:43,730] INFO Starting job: show at IcebergWriter.scala:117 (org.apache.spark.SparkContext)
[2022-03-04 21:50:43,731] INFO Got job 1 (show at IcebergWriter.scala:117) with 2 output partitions (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:43,731] INFO Final stage: ResultStage 1 (show at IcebergWriter.scala:117) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:43,731] INFO Parents of final stage: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:43,731] INFO Missing parents: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:43,732] INFO Submitting ResultStage 1 (MapPartitionsRDD[4] at show at IcebergWriter.scala:117), which has no missing parents (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:43,737] INFO Block broadcast_2 stored as values in memory (estimated size 51.9 KiB, free 365.9 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:43,738] INFO Block broadcast_2_piece0 stored as bytes in memory (estimated size 15.2 KiB, free 365.9 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:43,739] INFO Added broadcast_2_piece0 in memory on hadoop:40849 (size: 15.2 KiB, free: 366.3 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:43,739] INFO Created broadcast 2 from broadcast at DAGScheduler.scala:1478 (org.apache.spark.SparkContext)
[2022-03-04 21:50:43,740] INFO Submitting 2 missing tasks from ResultStage 1 (MapPartitionsRDD[4] at show at IcebergWriter.scala:117) (first 15 tasks are for partitions Vector(1, 2)) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:43,740] INFO Adding task set 1.0 with 2 tasks resource profile 0 (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:50:43,741] INFO Starting task 0.0 in stage 1.0 (TID 1) (hadoop, executor 1, partition 1, PROCESS_LOCAL, 4368 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:43,752] INFO Added broadcast_2_piece0 in memory on hadoop:46193 (size: 15.2 KiB, free: 366.3 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:44,594] INFO Starting task 1.0 in stage 1.0 (TID 2) (hadoop, executor 1, partition 2, PROCESS_LOCAL, 4368 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:44,595] INFO Finished task 0.0 in stage 1.0 (TID 1) in 854 ms on hadoop (executor 1) (1/2) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:44,665] INFO Finished task 1.0 in stage 1.0 (TID 2) in 72 ms on hadoop (executor 1) (2/2) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:44,666] INFO Removed TaskSet 1.0, whose tasks have all completed, from pool  (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:50:44,666] INFO ResultStage 1 (show at IcebergWriter.scala:117) finished in 0.932 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:44,667] INFO Job 1 is finished. Cancelling potential speculative or zombie tasks for this job (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:44,667] INFO Killing all running tasks in stage 1: Stage finished (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:50:44,667] INFO Job 1 finished: show at IcebergWriter.scala:117, took 0.936544 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:44,714] INFO Code generated in 27.126375 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:50:44,771] INFO Hadoop catalog [HadoopCatalog{name=hadoop, location=hdfs://hadoop:8020/user/test/iceberg}], namespace [db_gb18030_test] not exists, try to create namespace (org.apache.iceberg.streaming.utils.HadoopUtils)
[2022-03-04 21:50:44,806] INFO Hadoop table not exists, start create namespace[db_gb18030_test]-table[hadoop.db_gb18030_test.tbl_test_1] with sql [
CREATE TABLE hadoop.db_gb18030_test.tbl_test_1 (
_src_name string, _src_db string, _src_table string, _src_ts_ms long, _src_server_id long, _src_file string, _src_pos long, _src_op string, _src_ts_ms_r long, _tsc_id string, _tsc_total_order long, _tsc_data_collection_order long, _kfk_topic string, _kfk_partition int, _kfk_offset long, _kfk_timestamp long, id1 int, id2 string, c1 string, c2 int, create_time long, update_time long)
USING iceberg
PARTITIONED BY (id1,id2)
COMMENT 'db_gb18030_test tbl_test_1'
TBLPROPERTIES ('read.split.target-size'='268435456','write.metadata.delete-after-commit.enabled'='true')
] (org.apache.iceberg.streaming.utils.HadoopUtils)
[2022-03-04 21:50:45,540] INFO Supplied authorities: hadoop:10000 (org.apache.hive.jdbc.Utils)
[2022-03-04 21:50:45,540] INFO Resolved authority: hadoop:10000 (org.apache.hive.jdbc.Utils)
[2022-03-04 21:50:46,727] INFO Create external hive catalog table with sql [
CREATE EXTERNAL TABLE IF NOT EXISTS db_gb18030_test.tbl_test_1
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
TBLPROPERTIES ('iceberg.catalog'='hadoop')
] (org.apache.iceberg.streaming.utils.HiveUtils)
[2022-03-04 21:50:48,033] INFO 
Pushing operators to hadoop.db_gb18030_test.tbl_test_1
Pushed filters: 
Filters that were not pushed: 
Output: _src_name#155, _src_db#156, _src_table#157, _src_ts_ms#158L, _src_server_id#159L, _src_file#160, _src_pos#161L, _src_op#162, _src_ts_ms_r#163L, _tsc_id#164, _tsc_total_order#165L, _tsc_data_collection_order#166L, _kfk_topic#167, _kfk_partition#168, _kfk_offset#169L, _kfk_timestamp#170L, id1#171, id2#172, c1#173, c2#174, create_time#175L, update_time#176L, _file#179
          (org.apache.spark.sql.execution.datasources.v2.RowLevelCommandScanRelationPushDown)
[2022-03-04 21:50:48,148] INFO Block broadcast_3 stored as values in memory (estimated size 164.8 KiB, free 365.8 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,154] INFO Block broadcast_3_piece0 stored as bytes in memory (estimated size 22.1 KiB, free 365.7 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,155] INFO Added broadcast_3_piece0 in memory on hadoop:40849 (size: 22.1 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,156] INFO Created broadcast 3 from broadcast at SparkBatchScan.java:139 (org.apache.spark.SparkContext)
[2022-03-04 21:50:48,171] INFO Block broadcast_4 stored as values in memory (estimated size 164.8 KiB, free 365.6 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,173] INFO Block broadcast_4_piece0 stored as bytes in memory (estimated size 22.1 KiB, free 365.6 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,173] INFO Added broadcast_4_piece0 in memory on hadoop:40849 (size: 22.1 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,174] INFO Created broadcast 4 from broadcast at SparkBatchScan.java:139 (org.apache.spark.SparkContext)
[2022-03-04 21:50:48,203] INFO Block broadcast_5 stored as values in memory (estimated size 164.8 KiB, free 365.4 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,205] INFO Block broadcast_5_piece0 stored as bytes in memory (estimated size 22.1 KiB, free 365.4 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,206] INFO Added broadcast_5_piece0 in memory on hadoop:40849 (size: 22.1 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,206] INFO Created broadcast 5 from broadcast at SparkBatchScan.java:139 (org.apache.spark.SparkContext)
[2022-03-04 21:50:48,266] INFO Block broadcast_6 stored as values in memory (estimated size 164.8 KiB, free 365.2 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,268] INFO Block broadcast_6_piece0 stored as bytes in memory (estimated size 22.1 KiB, free 365.2 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,268] INFO Added broadcast_6_piece0 in memory on hadoop:40849 (size: 22.1 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,269] INFO Created broadcast 6 from broadcast at SparkBatchScan.java:139 (org.apache.spark.SparkContext)
[2022-03-04 21:50:48,284] INFO Block broadcast_7 stored as values in memory (estimated size 164.8 KiB, free 365.0 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,286] INFO Block broadcast_7_piece0 stored as bytes in memory (estimated size 22.1 KiB, free 365.0 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,286] INFO Added broadcast_7_piece0 in memory on hadoop:40849 (size: 22.1 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,287] INFO Created broadcast 7 from broadcast at SparkBatchScan.java:139 (org.apache.spark.SparkContext)
[2022-03-04 21:50:48,334] INFO Code generated in 9.634125 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:50:48,344] INFO Code generated in 23.696459 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:50:48,356] INFO Code generated in 6.85175 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:50:48,366] INFO Registering RDD 9 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) as input to shuffle 0 (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,370] INFO Got map stage job 2 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) with 3 output partitions (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,370] INFO Final stage: ShuffleMapStage 2 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,370] INFO Parents of final stage: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,371] INFO Missing parents: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,373] INFO Submitting ShuffleMapStage 2 (MapPartitionsRDD[9] at $anonfun$withThreadLocalCaptured$1 at FutureTask.java:266), which has no missing parents (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,389] INFO Block broadcast_8 stored as values in memory (estimated size 40.0 B, free 365.0 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,390] INFO Block broadcast_8_piece0 stored as bytes in memory (estimated size 86.0 B, free 365.0 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,391] INFO Added broadcast_8_piece0 in memory on hadoop:40849 (size: 86.0 B, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,391] INFO Created broadcast 8 from sql at IcebergWriter.scala:61 (org.apache.spark.SparkContext)
[2022-03-04 21:50:48,396] INFO Block broadcast_9 stored as values in memory (estimated size 164.8 KiB, free 364.9 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,397] INFO Block broadcast_9_piece0 stored as bytes in memory (estimated size 22.1 KiB, free 364.8 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,398] INFO Added broadcast_9_piece0 in memory on hadoop:40849 (size: 22.1 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,399] INFO Created broadcast 9 from broadcast at SparkBatchScan.java:139 (org.apache.spark.SparkContext)
[2022-03-04 21:50:48,399] INFO Block broadcast_10 stored as values in memory (estimated size 47.5 KiB, free 364.8 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,400] INFO Block broadcast_10_piece0 stored as bytes in memory (estimated size 14.9 KiB, free 364.8 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,400] INFO Added broadcast_10_piece0 in memory on hadoop:40849 (size: 14.9 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,401] INFO Created broadcast 10 from broadcast at DAGScheduler.scala:1478 (org.apache.spark.SparkContext)
[2022-03-04 21:50:48,402] INFO Submitting 3 missing tasks from ShuffleMapStage 2 (MapPartitionsRDD[9] at $anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) (first 15 tasks are for partitions Vector(0, 1, 2)) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,402] INFO Adding task set 2.0 with 3 tasks resource profile 0 (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:50:48,404] INFO Starting task 0.0 in stage 2.0 (TID 3) (hadoop, executor 1, partition 0, PROCESS_LOCAL, 4357 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:48,442] INFO Added broadcast_10_piece0 in memory on hadoop:46193 (size: 14.9 KiB, free: 366.3 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,446] INFO Code generated in 20.980458 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:50:48,449] INFO Removed broadcast_7_piece0 on hadoop:40849 in memory (size: 22.1 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,450] INFO Registering RDD 14 (sql at IcebergWriter.scala:61) as input to shuffle 1 (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,450] INFO Got map stage job 3 (sql at IcebergWriter.scala:61) with 3 output partitions (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,450] INFO Final stage: ShuffleMapStage 3 (sql at IcebergWriter.scala:61) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,451] INFO Parents of final stage: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,451] INFO Missing parents: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,451] INFO Submitting ShuffleMapStage 3 (MapPartitionsRDD[14] at sql at IcebergWriter.scala:61), which has no missing parents (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,472] INFO Block broadcast_11 stored as values in memory (estimated size 53.0 KiB, free 364.9 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,474] INFO Block broadcast_11_piece0 stored as bytes in memory (estimated size 16.1 KiB, free 364.9 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:48,475] INFO Added broadcast_11_piece0 in memory on hadoop:40849 (size: 16.1 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,475] INFO Removed broadcast_5_piece0 on hadoop:40849 in memory (size: 22.1 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,475] INFO Created broadcast 11 from broadcast at DAGScheduler.scala:1478 (org.apache.spark.SparkContext)
[2022-03-04 21:50:48,476] INFO Submitting 3 missing tasks from ShuffleMapStage 3 (MapPartitionsRDD[14] at sql at IcebergWriter.scala:61) (first 15 tasks are for partitions Vector(0, 1, 2)) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,476] INFO Adding task set 3.0 with 3 tasks resource profile 0 (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:50:48,487] INFO Removed broadcast_9_piece0 on hadoop:40849 in memory (size: 22.1 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,506] INFO Removed broadcast_6_piece0 on hadoop:40849 in memory (size: 22.1 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,513] INFO Removed broadcast_3_piece0 on hadoop:40849 in memory (size: 22.1 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,522] INFO Removed broadcast_1_piece0 on hadoop:40849 in memory (size: 15.2 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,527] INFO Removed broadcast_1_piece0 on hadoop:46193 in memory (size: 15.2 KiB, free: 366.3 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,532] INFO Removed broadcast_4_piece0 on hadoop:40849 in memory (size: 22.1 KiB, free: 366.3 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,537] INFO Removed broadcast_2_piece0 on hadoop:40849 in memory (size: 15.2 KiB, free: 366.3 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,539] INFO Removed broadcast_2_piece0 on hadoop:46193 in memory (size: 15.2 KiB, free: 366.3 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:48,582] INFO Starting task 1.0 in stage 2.0 (TID 4) (hadoop, executor 1, partition 1, PROCESS_LOCAL, 4357 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:48,584] INFO Finished task 0.0 in stage 2.0 (TID 3) in 181 ms on hadoop (executor 1) (1/3) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:48,635] INFO Starting task 2.0 in stage 2.0 (TID 5) (hadoop, executor 1, partition 2, PROCESS_LOCAL, 4357 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:48,635] INFO Finished task 1.0 in stage 2.0 (TID 4) in 53 ms on hadoop (executor 1) (2/3) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:48,669] INFO Starting task 0.0 in stage 3.0 (TID 6) (hadoop, executor 1, partition 0, PROCESS_LOCAL, 4357 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:48,670] INFO Finished task 2.0 in stage 2.0 (TID 5) in 35 ms on hadoop (executor 1) (3/3) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:48,670] INFO Removed TaskSet 2.0, whose tasks have all completed, from pool  (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:50:48,670] INFO ShuffleMapStage 2 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) finished in 0.294 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,671] INFO looking for newly runnable stages (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,671] INFO running: Set(ShuffleMapStage 3) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,671] INFO waiting: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,671] INFO failed: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:48,680] INFO Added broadcast_11_piece0 in memory on hadoop:46193 (size: 16.1 KiB, free: 366.3 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:49,037] INFO Starting task 1.0 in stage 3.0 (TID 7) (hadoop, executor 1, partition 1, PROCESS_LOCAL, 4357 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:49,037] INFO Finished task 0.0 in stage 3.0 (TID 6) in 368 ms on hadoop (executor 1) (1/3) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:49,116] INFO Starting task 2.0 in stage 3.0 (TID 8) (hadoop, executor 1, partition 2, PROCESS_LOCAL, 4357 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:49,117] INFO Finished task 1.0 in stage 3.0 (TID 7) in 80 ms on hadoop (executor 1) (2/3) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:49,169] INFO Finished task 2.0 in stage 3.0 (TID 8) in 53 ms on hadoop (executor 1) (3/3) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:49,169] INFO Removed TaskSet 3.0, whose tasks have all completed, from pool  (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:50:49,170] INFO ShuffleMapStage 3 (sql at IcebergWriter.scala:61) finished in 0.715 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:49,170] INFO looking for newly runnable stages (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:49,170] INFO running: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:49,170] INFO waiting: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:49,170] INFO failed: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:49,179] INFO For shuffle(1), advisory target size: 67108864, actual target size 1048576, minimum partition size: 1048576 (org.apache.spark.sql.execution.adaptive.ShufflePartitionsUtil)
[2022-03-04 21:50:49,192] WARN Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'. (org.apache.spark.sql.catalyst.util.package)
[2022-03-04 21:50:49,221] INFO Code generated in 21.923833 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:50:49,249] INFO Code generated in 18.431334 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:50:49,281] INFO Block broadcast_12 stored as values in memory (estimated size 164.8 KiB, free 365.8 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:49,282] INFO Block broadcast_12_piece0 stored as bytes in memory (estimated size 22.1 KiB, free 365.7 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:49,283] INFO Added broadcast_12_piece0 in memory on hadoop:40849 (size: 22.1 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:49,283] INFO Created broadcast 12 from broadcast at SparkWrite.java:173 (org.apache.spark.SparkContext)
[2022-03-04 21:50:49,285] INFO Start processing data source write support: IcebergBatchWrite(table=hadoop.db_gb18030_test.tbl_test_1, format=PARQUET). The input RDD has 1 partitions. (org.apache.spark.sql.execution.datasources.v2.ReplaceDataExec)
[2022-03-04 21:50:49,290] INFO Starting job: sql at IcebergWriter.scala:61 (org.apache.spark.SparkContext)
[2022-03-04 21:50:49,291] INFO Got job 4 (sql at IcebergWriter.scala:61) with 1 output partitions (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:49,291] INFO Final stage: ResultStage 5 (sql at IcebergWriter.scala:61) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:49,291] INFO Parents of final stage: List(ShuffleMapStage 4) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:49,292] INFO Missing parents: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:49,293] INFO Submitting ResultStage 5 (MapPartitionsRDD[18] at sql at IcebergWriter.scala:61), which has no missing parents (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:49,301] INFO Block broadcast_13 stored as values in memory (estimated size 89.1 KiB, free 365.7 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:49,302] INFO Block broadcast_13_piece0 stored as bytes in memory (estimated size 27.2 KiB, free 365.6 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:49,302] INFO Added broadcast_13_piece0 in memory on hadoop:40849 (size: 27.2 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:49,303] INFO Created broadcast 13 from broadcast at DAGScheduler.scala:1478 (org.apache.spark.SparkContext)
[2022-03-04 21:50:49,303] INFO Submitting 1 missing tasks from ResultStage 5 (MapPartitionsRDD[18] at sql at IcebergWriter.scala:61) (first 15 tasks are for partitions Vector(0)) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:49,303] INFO Adding task set 5.0 with 1 tasks resource profile 0 (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:50:49,305] INFO Starting task 0.0 in stage 5.0 (TID 9) (hadoop, executor 1, partition 0, NODE_LOCAL, 4442 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:49,316] INFO Added broadcast_13_piece0 in memory on hadoop:46193 (size: 27.2 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:49,381] INFO Asked to send map output locations for shuffle 1 to 192.168.100.60:49536 (org.apache.spark.MapOutputTrackerMasterEndpoint)
[2022-03-04 21:50:49,576] INFO Added broadcast_12_piece0 in memory on hadoop:46193 (size: 22.1 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:51,690] INFO Finished task 0.0 in stage 5.0 (TID 9) in 2386 ms on hadoop (executor 1) (1/1) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:50:51,690] INFO Removed TaskSet 5.0, whose tasks have all completed, from pool  (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:50:51,690] INFO ResultStage 5 (sql at IcebergWriter.scala:61) finished in 2.393 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:51,691] INFO Job 4 is finished. Cancelling potential speculative or zombie tasks for this job (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:51,691] INFO Killing all running tasks in stage 5: Stage finished (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:50:51,691] INFO Job 4 finished: sql at IcebergWriter.scala:61, took 2.400586 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:50:51,692] INFO Data source write support IcebergBatchWrite(table=hadoop.db_gb18030_test.tbl_test_1, format=PARQUET) is committing. (org.apache.spark.sql.execution.datasources.v2.ReplaceDataExec)
[2022-03-04 21:50:51,704] INFO Committing overwrite of 0 data files with 3 new data files, scanSnapshotId: null, conflictDetectionFilter: true to table hadoop.db_gb18030_test.tbl_test_1 (org.apache.iceberg.spark.source.SparkWrite)
[2022-03-04 21:50:52,689] INFO Committed snapshot 1630447149345993327 (BaseOverwriteFiles) (org.apache.iceberg.SnapshotProducer)
[2022-03-04 21:50:52,737] INFO Committed in 1033 ms (org.apache.iceberg.spark.source.SparkWrite)
[2022-03-04 21:50:52,737] INFO Data source write support IcebergBatchWrite(table=hadoop.db_gb18030_test.tbl_test_1, format=PARQUET) committed. (org.apache.spark.sql.execution.datasources.v2.ReplaceDataExec)
[2022-03-04 21:50:52,739] INFO finished write data into hadoop.db_gb18030_test.tbl_test_1 ... (org.apache.iceberg.streaming.write.IcebergWriter)
[2022-03-04 21:50:52,739] INFO commit offset rangers: [OffsetRange(topic: 'test.db_gb18030_test.tbl_test_1', partition: 0, range: [0 -> 0]),OffsetRange(topic: 'test.db_gb18030_test.tbl_test_1', partition: 1, range: [0 -> 3]),OffsetRange(topic: 'test.db_gb18030_test.tbl_test_1', partition: 2, range: [0 -> 4])] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:50:52,740] INFO ----------------------------------------------------------------------------------
 (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:50:52,741] INFO after write accumulator value [
        StatusAccumulator(
            partitionOffsets: Map(
                test.db_gb18030_test.tbl_test_1:0 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 0, range: [0 -> 4], curOffset: 0),
                test.db_gb18030_test.tbl_test_1:1 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 1, range: [0 -> 4], curOffset: 3),
                test.db_gb18030_test.tbl_test_1:2 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 2, range: [0 -> 18], curOffset: 4)
                ),
            schemaVersion: 1
        )
] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:50:52,743] INFO Finished job streaming job 1646401840000 ms.0 from job set of time 1646401840000 ms (org.apache.spark.streaming.scheduler.JobScheduler)
[2022-03-04 21:50:52,744] ERROR Error running job streaming job 1646401840000 ms.0 (org.apache.spark.streaming.scheduler.JobScheduler)
org.apache.iceberg.streaming.exception.SchemaChangedException: detect schema version changed, need restart spark streaming job...
	at org.apache.iceberg.streaming.Kafka2Iceberg$.$anonfun$startStreamJob$1(Kafka2Iceberg.scala:221)
	at org.apache.iceberg.streaming.Kafka2Iceberg$.$anonfun$startStreamJob$1$adapted(Kafka2Iceberg.scala:173)
	at org.apache.spark.streaming.dstream.DStream.$anonfun$foreachRDD$2(DStream.scala:629)
	at org.apache.spark.streaming.dstream.DStream.$anonfun$foreachRDD$2$adapted(DStream.scala:629)
	at org.apache.spark.streaming.dstream.ForEachDStream.$anonfun$generateJob$2(ForEachDStream.scala:51)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
	at org.apache.spark.streaming.dstream.DStream.createRDDWithLocalProperties(DStream.scala:417)
	at org.apache.spark.streaming.dstream.ForEachDStream.$anonfun$generateJob$1(ForEachDStream.scala:51)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
	at scala.util.Try$.apply(Try.scala:213)
	at org.apache.spark.streaming.scheduler.Job.run(Job.scala:39)
	at org.apache.spark.streaming.scheduler.JobScheduler$JobHandler.$anonfun$run$1(JobScheduler.scala:256)
	at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
	at scala.util.DynamicVariable.withValue(DynamicVariable.scala:62)
	at org.apache.spark.streaming.scheduler.JobScheduler$JobHandler.run(JobScheduler.scala:256)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
[2022-03-04 21:50:52,747] WARN Detect Schema Version Changed, Restart SparkStream Job... (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:50:52,749] INFO ReceiverTracker stopped (org.apache.spark.streaming.scheduler.ReceiverTracker)
[2022-03-04 21:50:52,749] INFO Stopping JobGenerator immediately (org.apache.spark.streaming.scheduler.JobGenerator)
[2022-03-04 21:50:52,750] INFO Stopped timer for JobGenerator after time 1646401840000 (org.apache.spark.streaming.util.RecurringTimer)
[2022-03-04 21:50:52,751] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Revoke previously assigned partitions test.db_gb18030_test.tbl_test_1-0, test.db_gb18030_test.tbl_test_1-1, test.db_gb18030_test.tbl_test_1-2 (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:52,751] INFO [Consumer clientId=consumer-iceberg-hadoop-3, groupId=iceberg-hadoop] Member consumer-iceberg-hadoop-3-79a4d99f-ec82-4732-818d-953ffbc7eb22 sending LeaveGroup request to coordinator 192.168.100.30:9092 (id: 2147483647 rack: null) due to the consumer is being closed (org.apache.kafka.clients.consumer.internals.AbstractCoordinator)
[2022-03-04 21:50:52,755] INFO Metrics scheduler closed (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:52,756] INFO Closing reporter org.apache.kafka.common.metrics.JmxReporter (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:52,756] INFO Metrics reporters closed (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:52,757] INFO App info kafka.consumer for consumer-iceberg-hadoop-3 unregistered (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:52,758] INFO Stopped JobGenerator (org.apache.spark.streaming.scheduler.JobGenerator)
[2022-03-04 21:50:52,760] INFO Stopped JobScheduler (org.apache.spark.streaming.scheduler.JobScheduler)
[2022-03-04 21:50:52,764] INFO Stopped o.s.j.s.ServletContextHandler@3a029522{/streaming,null,STOPPED,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:52,764] INFO Stopped o.s.j.s.ServletContextHandler@32891eed{/streaming/json,null,STOPPED,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:52,764] INFO Stopped o.s.j.s.ServletContextHandler@4f7a19c3{/streaming/batch,null,STOPPED,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:52,765] INFO Stopped o.s.j.s.ServletContextHandler@2585635a{/streaming/batch/json,null,STOPPED,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:52,765] INFO Stopped o.s.j.s.ServletContextHandler@6e223945{/static/streaming,null,STOPPED,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:52,766] INFO StreamingContext stopped successfully (org.apache.spark.streaming.StreamingContext)
[2022-03-04 21:50:55,769] INFO commit kafka offset by kafka client [test.db_gb18030_test.tbl_test_1-0 -> OffsetAndMetadata{offset=0, leaderEpoch=null, metadata=''},test.db_gb18030_test.tbl_test_1-1 -> OffsetAndMetadata{offset=3, leaderEpoch=null, metadata=''},test.db_gb18030_test.tbl_test_1-2 -> OffsetAndMetadata{offset=4, leaderEpoch=null, metadata=''}] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:50:55,769] INFO ConsumerConfig values: 

 (io.confluent.kafka.serializers.KafkaAvroDeserializerConfig)
[2022-03-04 21:50:55,772] INFO Kafka version: 2.8.0 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:55,772] INFO Kafka commitId: ebb1d6e21cc92130 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:55,772] INFO Kafka startTimeMs: 1646401855772 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:55,775] INFO [Consumer clientId=consumer-iceberg-hadoop-4, groupId=iceberg-hadoop] Cluster ID: z9nnmK3URTSygqYnuIgGJg (org.apache.kafka.clients.Metadata)
[2022-03-04 21:50:55,775] INFO [Consumer clientId=consumer-iceberg-hadoop-4, groupId=iceberg-hadoop] Discovered group coordinator 192.168.100.30:9092 (id: 2147483647 rack: null) (org.apache.kafka.clients.consumer.internals.AbstractCoordinator)
[2022-03-04 21:50:55,801] INFO Metrics scheduler closed (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:55,801] INFO Closing reporter org.apache.kafka.common.metrics.JmxReporter (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:55,801] INFO Metrics reporters closed (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:55,802] INFO App info kafka.consumer for consumer-iceberg-hadoop-4 unregistered (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:55,802] INFO Restarted initAccumulatorAndBroadcast ... (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:50:55,802] INFO Table Configure [spark.sql.warehouse.dir -> hdfs://hadoop:8020/user/hive/warehouse, iceberg.table.partitionBy -> id1,id2, kafka.consumer.topic -> test.db_gb18030_test.tbl_test_1, iceberg.table.comment -> db_gb18030_test tbl_test_1, spark.hadoop.hive.metastore.uris -> thrift://hadoop:9083, hive.jdbc.url -> jdbc:hive2://hadoop:10000, iceberg.table.properties -> 'read.split.target-size'='268435456','write.metadata.delete-after-commit.enabled'='true', record.metadata.source.columns -> name, db, table, ts_ms, server_id, file, pos, kafka.consumer.max.poll.records -> 5, spark.sql.extensions -> org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions, iceberg.table.name -> hadoop.db_gb18030_test.tbl_test_1, kafka.bootstrap.servers -> kafka:9092, record.metadata.source.prefix -> _src_, record.metadata.kafka.columns -> topic, partition, offset, timestamp, kafka.consumer.commit.timeout.millis -> 60000, spark.sql.sources.partitionOverwriteMode -> dynamic, hive.jdbc.password -> , kafka.consumer.group.id -> iceberg-hadoop, hive.jdbc.user -> , spark.streaming.kafka.maxRatePerPartition -> 10000, kafka.consumer.key.deserializer -> io.confluent.kafka.serializers.KafkaAvroDeserializer, spark.sql.catalog.hadoop.type -> hadoop, record.metadata.transaction.prefix -> _tsc_, spark.yarn.jars -> hdfs:/user/share/libs/spark/3.2.1/*.jar,hdfs:/user/share/libs/kafka2iceberg/0.1.0/*.jar, record.metadata.kafka.prefix -> _kfk_, kafka.schema.registry.url -> http://kafka:8081, spark.master -> yarn, hive.external.jar -> hdfs://hadoop:8020/user/share/libs/iceberg-runtime/0.13.1/iceberg-hive-runtime-0.13.1.jar, spark.sql.catalog.hadoop -> org.apache.iceberg.spark.SparkCatalog, kafka.consumer.value.deserializer -> io.confluent.kafka.serializers.KafkaAvroDeserializer, iceberg.table.primaryKey -> id1,id2, spark.app.name -> Kafka2Iceberg, spark.sql.catalog.hadoop.warehouse -> hdfs://hadoop:8020/user/test/iceberg, record.metadata.transaction.columns -> id, total_order, data_collection_order] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:50:55,802] INFO Get next batch min schema version by kafka commit offset, because StatusAccumulator status is null (org.apache.iceberg.streaming.utils.SchemaUtils)
[2022-03-04 21:50:55,802] INFO ConsumerConfig values: 

[2022-03-04 21:50:55,804] INFO Kafka version: 2.8.0 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:55,804] INFO Kafka commitId: ebb1d6e21cc92130 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:55,804] INFO Kafka startTimeMs: 1646401855804 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:55,806] INFO [Consumer clientId=consumer-iceberg-hadoop-5, groupId=iceberg-hadoop] Cluster ID: z9nnmK3URTSygqYnuIgGJg (org.apache.kafka.clients.Metadata)
[2022-03-04 21:50:55,810] INFO [Consumer clientId=consumer-iceberg-hadoop-5, groupId=iceberg-hadoop] Discovered group coordinator 192.168.100.30:9092 (id: 2147483647 rack: null) (org.apache.kafka.clients.consumer.internals.AbstractCoordinator)
[2022-03-04 21:50:55,811] INFO [Consumer clientId=consumer-iceberg-hadoop-5, groupId=iceberg-hadoop] Subscribed to partition(s): test.db_gb18030_test.tbl_test_1-0 (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:55,811] INFO [Consumer clientId=consumer-iceberg-hadoop-5, groupId=iceberg-hadoop] Seeking to offset 0 for partition test.db_gb18030_test.tbl_test_1-0 (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:55,811] INFO load partition [test.db_gb18030_test.tbl_test_1-0] schema info from offset [0] (org.apache.iceberg.streaming.utils.SchemaUtils)
[2022-03-04 21:50:55,825] INFO [Consumer clientId=consumer-iceberg-hadoop-5, groupId=iceberg-hadoop] Unsubscribed all topics or patterns and assigned partitions (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:55,825] INFO [Consumer clientId=consumer-iceberg-hadoop-5, groupId=iceberg-hadoop] Subscribed to partition(s): test.db_gb18030_test.tbl_test_1-1 (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:55,825] INFO [Consumer clientId=consumer-iceberg-hadoop-5, groupId=iceberg-hadoop] Seeking to offset 3 for partition test.db_gb18030_test.tbl_test_1-1 (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:55,825] INFO load partition [test.db_gb18030_test.tbl_test_1-1] schema info from offset [3] (org.apache.iceberg.streaming.utils.SchemaUtils)
[2022-03-04 21:50:56,361] INFO [Consumer clientId=consumer-iceberg-hadoop-5, groupId=iceberg-hadoop] Unsubscribed all topics or patterns and assigned partitions (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:56,361] INFO [Consumer clientId=consumer-iceberg-hadoop-5, groupId=iceberg-hadoop] Subscribed to partition(s): test.db_gb18030_test.tbl_test_1-2 (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:56,361] INFO [Consumer clientId=consumer-iceberg-hadoop-5, groupId=iceberg-hadoop] Seeking to offset 4 for partition test.db_gb18030_test.tbl_test_1-2 (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:56,361] INFO load partition [test.db_gb18030_test.tbl_test_1-2] schema info from offset [4] (org.apache.iceberg.streaming.utils.SchemaUtils)
[2022-03-04 21:50:56,856] INFO [Consumer clientId=consumer-iceberg-hadoop-5, groupId=iceberg-hadoop] Unsubscribed all topics or patterns and assigned partitions (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:56,856] INFO Metrics scheduler closed (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:56,856] INFO Closing reporter org.apache.kafka.common.metrics.JmxReporter (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:56,857] INFO Metrics reporters closed (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:56,857] INFO App info kafka.consumer for consumer-iceberg-hadoop-5 unregistered (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:56,857] INFO Kafka partition schema version [test.db_gb18030_test.tbl_test_1-1 -> 2,test.db_gb18030_test.tbl_test_1-0 -> 4,test.db_gb18030_test.tbl_test_1-2 -> 2] (org.apache.iceberg.streaming.utils.SchemaUtils)
[2022-03-04 21:50:56,859] INFO Set next bach schema version to min schema version of unfinished partition [test.db_gb18030_test.tbl_test_1-1 -> 2, test.db_gb18030_test.tbl_test_1-0 -> 4, test.db_gb18030_test.tbl_test_1-2 -> 2]  (org.apache.iceberg.streaming.utils.SchemaUtils)
[2022-03-04 21:50:56,859] INFO ConsumerConfig values: 

 (io.confluent.kafka.serializers.KafkaAvroDeserializerConfig)
[2022-03-04 21:50:56,861] INFO Kafka version: 2.8.0 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:56,861] INFO Kafka commitId: ebb1d6e21cc92130 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:56,861] INFO Kafka startTimeMs: 1646401856861 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:56,863] INFO [Consumer clientId=consumer-iceberg-hadoop-6, groupId=iceberg-hadoop] Cluster ID: z9nnmK3URTSygqYnuIgGJg (org.apache.kafka.clients.Metadata)
[2022-03-04 21:50:56,866] INFO [Consumer clientId=consumer-iceberg-hadoop-6, groupId=iceberg-hadoop] Discovered group coordinator 192.168.100.30:9092 (id: 2147483647 rack: null) (org.apache.kafka.clients.consumer.internals.AbstractCoordinator)
[2022-03-04 21:50:56,870] INFO Metrics scheduler closed (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:56,870] INFO Closing reporter org.apache.kafka.common.metrics.JmxReporter (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:56,870] INFO Metrics reporters closed (org.apache.kafka.common.metrics.Metrics)
[2022-03-04 21:50:56,871] INFO App info kafka.consumer for consumer-iceberg-hadoop-6 unregistered (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:56,901] INFO Block broadcast_14 stored as values in memory (estimated size 245.6 KiB, free 365.4 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:56,903] INFO Block broadcast_14_piece0 stored as bytes in memory (estimated size 1460.0 B, free 365.4 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:50:56,903] INFO Added broadcast_14_piece0 in memory on hadoop:40849 (size: 1460.0 B, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:50:56,904] INFO Created broadcast 14 from broadcast at Kafka2Iceberg.scala:288 (org.apache.spark.SparkContext)
[2022-03-04 21:50:56,904] INFO After Init StatusAccumulator [
Map(
    hadoop.db_gb18030_test.tbl_test_1 -> 
        StatusAccumulator(
            partitionOffsets: Map(
                test.db_gb18030_test.tbl_test_1:0 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 0, range: [0 -> 0], curOffset: 0),
                test.db_gb18030_test.tbl_test_1:1 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 1, range: [3 -> 0], curOffset: 3),
                test.db_gb18030_test.tbl_test_1:2 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 2, range: [4 -> 0], curOffset: 4)
                ),
            schemaVersion: 2
        )
)] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:50:56,905] INFO After Init SchemaBroadcast   [
Map(
    hadoop.db_gb18030_test.tbl_test_1 -> 
        SchemaBroadcast(
            schemaToVersionMap: Map(
                {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"C3","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"} -> 2,
                {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"} -> 1,
                {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"C4","type":["null","int"],"default":null},{"name":"C3","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"} -> 4,
                {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"C6","type":["null","long"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"C4","type":["null","int"],"default":null},{"name":"C3","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"} -> 5,
                {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"C4","type":["null","int"],"default":null},{"name":"C3","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"} -> 3),
            versionToSchemaMap: Map(
                5 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"C6","type":["null","long"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"C4","type":["null","int"],"default":null},{"name":"C3","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"},
                1 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"},
                2 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"C3","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"},
                3 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"C4","type":["null","int"],"default":null},{"name":"C3","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"},
                4 -> {"type":"record","name":"Envelope","namespace":"test.db_gb18030_test.tbl_test_1","fields":[{"name":"before","type":["null",{"type":"record","name":"Value","fields":[{"name":"ID1","type":"int"},{"name":"ID2","type":"string"},{"name":"C1","type":{"type":"string","connect.default":"CV1"},"default":"CV1"},{"name":"C2","type":["null","int"],"default":null},{"name":"C5","type":["null","string"],"default":null},{"name":"C4","type":["null","int"],"default":null},{"name":"C3","type":["null","string"],"default":null},{"name":"CREATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0},{"name":"UPDATE_TIME","type":{"type":"long","connect.version":1,"connect.default":0,"connect.name":"io.debezium.time.Timestamp"},"default":0}],"connect.name":"test.db_gb18030_test.tbl_test_1.Value"}],"default":null},{"name":"after","type":["null","Value"],"default":null},{"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.mysql","fields":[{"name":"version","type":"string"},{"name":"connector","type":"string"},{"name":"name","type":"string"},{"name":"ts_ms","type":"long"},{"name":"snapshot","type":[{"type":"string","connect.version":1,"connect.parameters":{"allowed":"true,last,false,incremental"},"connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],"default":"false"},{"name":"db","type":"string"},{"name":"sequence","type":["null","string"],"default":null},{"name":"table","type":["null","string"],"default":null},{"name":"server_id","type":"long"},{"name":"gtid","type":["null","string"],"default":null},{"name":"file","type":"string"},{"name":"pos","type":"long"},{"name":"row","type":"int"},{"name":"thread","type":["null","long"],"default":null},{"name":"query","type":["null","string"],"default":null}],"connect.name":"io.debezium.connector.mysql.Source"}},{"name":"op","type":"string"},{"name":"ts_ms","type":["null","long"],"default":null},{"name":"transaction","type":["null",{"type":"record","name":"ConnectDefault","namespace":"io.confluent.connect.avro","fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},{"name":"data_collection_order","type":"long"}]}],"default":null}],"connect.name":"test.db_gb18030_test.tbl_test_1.Envelope"})
            )
)] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:50:56,914] INFO Table loaded by catalog: hadoop.db_gb18030_test.tbl_test_1 (org.apache.iceberg.BaseMetastoreCatalog)
[2022-03-04 21:50:56,917] INFO Table [hadoop.db_gb18030_test.tbl_test_1] schema changed, before [table {
  1: _src_name: optional string
  2: _src_db: optional string
  3: _src_table: optional string
  4: _src_ts_ms: optional long
  5: _src_server_id: optional long
  6: _src_file: optional string
  7: _src_pos: optional long
  8: _src_op: optional string
  9: _src_ts_ms_r: optional long
  10: _tsc_id: optional string
  11: _tsc_total_order: optional long
  12: _tsc_data_collection_order: optional long
  13: _kfk_topic: optional string
  14: _kfk_partition: optional int
  15: _kfk_offset: optional long
  16: _kfk_timestamp: optional long
  17: id1: optional int
  18: id2: optional string
  19: c1: optional string
  20: c2: optional int
  21: create_time: optional long
  22: update_time: optional long
}] (org.apache.iceberg.streaming.utils.HadoopUtils)
[2022-03-04 21:50:56,928] INFO Try to alter table to table {
  1: _src_name: optional string
  2: _src_db: optional string
  3: _src_table: optional string
  4: _src_ts_ms: optional long
  5: _src_server_id: optional long
  6: _src_file: optional string
  7: _src_pos: optional long
  8: _src_op: optional string
  9: _src_ts_ms_r: optional long
  10: _tsc_id: optional string
  11: _tsc_total_order: optional long
  12: _tsc_data_collection_order: optional long
  13: _kfk_topic: optional string
  14: _kfk_partition: optional int
  15: _kfk_offset: optional long
  16: _kfk_timestamp: optional long
  17: id1: optional int
  18: id2: optional string
  19: c1: optional string
  20: c2: optional int
  23: c3: optional string
  21: create_time: optional long
  22: update_time: optional long
} (org.apache.iceberg.streaming.utils.HadoopUtils)
[2022-03-04 21:50:57,766] INFO Table [hadoop.db_gb18030_test.tbl_test_1] schema changed success  (org.apache.iceberg.streaming.utils.HadoopUtils)
[2022-03-04 21:50:57,767] INFO Restarted StreamingContext ... (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:50:57,768] WARN overriding enable.auto.commit to false for executor (org.apache.spark.streaming.kafka010.KafkaUtils)
[2022-03-04 21:50:57,768] WARN overriding auto.offset.reset to none for executor (org.apache.spark.streaming.kafka010.KafkaUtils)
[2022-03-04 21:50:57,768] WARN overriding executor group.id to spark-executor-iceberg-hadoop (org.apache.spark.streaming.kafka010.KafkaUtils)
[2022-03-04 21:50:57,768] WARN overriding receive.buffer.bytes to 65536 see KAFKA-3135 (org.apache.spark.streaming.kafka010.KafkaUtils)
[2022-03-04 21:50:57,772] INFO Slide time = 20000 ms (org.apache.spark.streaming.kafka010.DirectKafkaInputDStream)
[2022-03-04 21:50:57,772] INFO Storage level = Serialized 1x Replicated (org.apache.spark.streaming.kafka010.DirectKafkaInputDStream)
[2022-03-04 21:50:57,772] INFO Checkpoint interval = null (org.apache.spark.streaming.kafka010.DirectKafkaInputDStream)
[2022-03-04 21:50:57,772] INFO Remember interval = 20000 ms (org.apache.spark.streaming.kafka010.DirectKafkaInputDStream)
[2022-03-04 21:50:57,772] INFO Initialized and validated org.apache.spark.streaming.kafka010.DirectKafkaInputDStream@38721e20 (org.apache.spark.streaming.kafka010.DirectKafkaInputDStream)
[2022-03-04 21:50:57,772] INFO Slide time = 20000 ms (org.apache.spark.streaming.dstream.ForEachDStream)
[2022-03-04 21:50:57,772] INFO Storage level = Serialized 1x Replicated (org.apache.spark.streaming.dstream.ForEachDStream)
[2022-03-04 21:50:57,772] INFO Checkpoint interval = null (org.apache.spark.streaming.dstream.ForEachDStream)
[2022-03-04 21:50:57,772] INFO Remember interval = 20000 ms (org.apache.spark.streaming.dstream.ForEachDStream)
[2022-03-04 21:50:57,772] INFO Initialized and validated org.apache.spark.streaming.dstream.ForEachDStream@36b5f75e (org.apache.spark.streaming.dstream.ForEachDStream)
[2022-03-04 21:50:57,774] INFO ConsumerConfig values: 

[2022-03-04 21:50:57,775] INFO Kafka version: 2.8.0 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:57,775] INFO Kafka commitId: ebb1d6e21cc92130 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:57,775] INFO Kafka startTimeMs: 1646401857775 (org.apache.kafka.common.utils.AppInfoParser)
[2022-03-04 21:50:57,776] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] Subscribed to topic(s): test.db_gb18030_test.tbl_test_1 (org.apache.kafka.clients.consumer.KafkaConsumer)
[2022-03-04 21:50:57,778] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] Cluster ID: z9nnmK3URTSygqYnuIgGJg (org.apache.kafka.clients.Metadata)
[2022-03-04 21:50:57,778] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] Discovered group coordinator 192.168.100.30:9092 (id: 2147483647 rack: null) (org.apache.kafka.clients.consumer.internals.AbstractCoordinator)
[2022-03-04 21:50:57,778] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] (Re-)joining group (org.apache.kafka.clients.consumer.internals.AbstractCoordinator)
[2022-03-04 21:50:57,780] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] (Re-)joining group (org.apache.kafka.clients.consumer.internals.AbstractCoordinator)
[2022-03-04 21:50:57,782] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] Successfully joined group with generation Generation{generationId=11, memberId='consumer-iceberg-hadoop-7-2e73a9ca-4fd4-4171-bb17-fdab230300a3', protocol='range'} (org.apache.kafka.clients.consumer.internals.AbstractCoordinator)
[2022-03-04 21:50:57,782] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] Finished assignment for group at generation 11: {consumer-iceberg-hadoop-7-2e73a9ca-4fd4-4171-bb17-fdab230300a3=Assignment(partitions=[test.db_gb18030_test.tbl_test_1-0, test.db_gb18030_test.tbl_test_1-1, test.db_gb18030_test.tbl_test_1-2])} (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:57,785] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] Successfully synced group in generation Generation{generationId=11, memberId='consumer-iceberg-hadoop-7-2e73a9ca-4fd4-4171-bb17-fdab230300a3', protocol='range'} (org.apache.kafka.clients.consumer.internals.AbstractCoordinator)
[2022-03-04 21:50:57,785] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] Notifying assignor about the new Assignment(partitions=[test.db_gb18030_test.tbl_test_1-0, test.db_gb18030_test.tbl_test_1-1, test.db_gb18030_test.tbl_test_1-2]) (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:57,785] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] Adding newly assigned partitions: test.db_gb18030_test.tbl_test_1-0, test.db_gb18030_test.tbl_test_1-1, test.db_gb18030_test.tbl_test_1-2 (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:57,787] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] Setting offset for partition test.db_gb18030_test.tbl_test_1-0 to the committed offset FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}} (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:57,787] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] Setting offset for partition test.db_gb18030_test.tbl_test_1-1 to the committed offset FetchPosition{offset=3, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}} (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:57,787] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] Setting offset for partition test.db_gb18030_test.tbl_test_1-2 to the committed offset FetchPosition{offset=4, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}} (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
[2022-03-04 21:50:57,788] INFO Started timer for JobGenerator at time 1646401860000 (org.apache.spark.streaming.util.RecurringTimer)
[2022-03-04 21:50:57,788] INFO Started JobGenerator at 1646401860000 ms (org.apache.spark.streaming.scheduler.JobGenerator)
[2022-03-04 21:50:57,788] INFO Started JobScheduler (org.apache.spark.streaming.scheduler.JobScheduler)
[2022-03-04 21:50:57,789] INFO Adding filter to /streaming: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:57,789] INFO Started o.s.j.s.ServletContextHandler@5fa1a255{/streaming,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:57,789] INFO Adding filter to /streaming/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:57,790] INFO Started o.s.j.s.ServletContextHandler@61e24f32{/streaming/json,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:57,790] INFO Adding filter to /streaming/batch: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:57,790] INFO Started o.s.j.s.ServletContextHandler@79ee67f9{/streaming/batch,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:57,790] INFO Adding filter to /streaming/batch/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:57,791] INFO Started o.s.j.s.ServletContextHandler@75cc92b1{/streaming/batch/json,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:57,791] INFO Adding filter to /static/streaming: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter (org.apache.spark.ui.ServerInfo)
[2022-03-04 21:50:57,791] INFO Started o.s.j.s.ServletContextHandler@244fa44c{/static/streaming,null,AVAILABLE,@Spark} (org.sparkproject.jetty.server.handler.ContextHandler)
[2022-03-04 21:50:57,791] INFO StreamingContext started (org.apache.spark.streaming.StreamingContext)
[2022-03-04 21:51:00,004] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] Seeking to LATEST offset of partition test.db_gb18030_test.tbl_test_1-2 (org.apache.kafka.clients.consumer.internals.SubscriptionState)
[2022-03-04 21:51:00,004] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] Seeking to LATEST offset of partition test.db_gb18030_test.tbl_test_1-1 (org.apache.kafka.clients.consumer.internals.SubscriptionState)
[2022-03-04 21:51:00,005] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] Seeking to LATEST offset of partition test.db_gb18030_test.tbl_test_1-0 (org.apache.kafka.clients.consumer.internals.SubscriptionState)
[2022-03-04 21:51:00,008] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] Resetting offset for partition test.db_gb18030_test.tbl_test_1-0 to position FetchPosition{offset=4, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}}. (org.apache.kafka.clients.consumer.internals.SubscriptionState)
[2022-03-04 21:51:00,008] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] Resetting offset for partition test.db_gb18030_test.tbl_test_1-1 to position FetchPosition{offset=4, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}}. (org.apache.kafka.clients.consumer.internals.SubscriptionState)
[2022-03-04 21:51:00,008] INFO [Consumer clientId=consumer-iceberg-hadoop-7, groupId=iceberg-hadoop] Resetting offset for partition test.db_gb18030_test.tbl_test_1-2 to position FetchPosition{offset=18, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[192.168.100.30:9092 (id: 0 rack: null)], epoch=0}}. (org.apache.kafka.clients.consumer.internals.SubscriptionState)
[2022-03-04 21:51:00,011] INFO Added jobs for time 1646401860000 ms (org.apache.spark.streaming.scheduler.JobScheduler)
[2022-03-04 21:51:00,013] INFO Starting job streaming job 1646401860000 ms.0 from job set of time 1646401860000 ms (org.apache.spark.streaming.scheduler.JobScheduler)
[2022-03-04 21:51:00,014] INFO ---------------------------------------------------------------------------------- (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:51:00,014] INFO before write accumulator value [
        StatusAccumulator(
            partitionOffsets: Map(
                test.db_gb18030_test.tbl_test_1:0 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 0, range: [0 -> 4], curOffset: 0),
                test.db_gb18030_test.tbl_test_1:1 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 1, range: [3 -> 4], curOffset: 3),
                test.db_gb18030_test.tbl_test_1:2 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 2, range: [4 -> 18], curOffset: 4)
                ),
            schemaVersion: 2
        )
] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:51:00,014] INFO current offset ranges: [OffsetRange(topic: 'test.db_gb18030_test.tbl_test_1', partition: 0, range: [0 -> 4]),OffsetRange(topic: 'test.db_gb18030_test.tbl_test_1', partition: 1, range: [3 -> 4]),OffsetRange(topic: 'test.db_gb18030_test.tbl_test_1', partition: 2, range: [4 -> 18])] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:51:00,014] INFO current partition offsets: [test.db_gb18030_test.tbl_test_1:0 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 0, range: [0 -> 4], curOffset: 0),test.db_gb18030_test.tbl_test_1:1 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 1, range: [3 -> 4], curOffset: 3),test.db_gb18030_test.tbl_test_1:2 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 2, range: [4 -> 18], curOffset: 4)] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:51:00,014] INFO beginning write data into hadoop.db_gb18030_test.tbl_test_1 ... (org.apache.iceberg.streaming.write.IcebergWriter)
[2022-03-04 21:51:00,091] INFO Code generated in 27.23225 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:51:00,096] INFO Starting job: show at IcebergWriter.scala:117 (org.apache.spark.SparkContext)
[2022-03-04 21:51:00,096] INFO Got job 5 (show at IcebergWriter.scala:117) with 1 output partitions (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,097] INFO Final stage: ResultStage 6 (show at IcebergWriter.scala:117) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,097] INFO Parents of final stage: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,097] INFO Missing parents: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,098] INFO Submitting ResultStage 6 (MapPartitionsRDD[23] at show at IcebergWriter.scala:117), which has no missing parents (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,134] INFO Block broadcast_15 stored as values in memory (estimated size 53.3 KiB, free 365.3 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,135] INFO Block broadcast_15_piece0 stored as bytes in memory (estimated size 15.4 KiB, free 365.3 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,135] INFO Added broadcast_15_piece0 in memory on hadoop:40849 (size: 15.4 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:00,135] INFO Created broadcast 15 from broadcast at DAGScheduler.scala:1478 (org.apache.spark.SparkContext)
[2022-03-04 21:51:00,136] INFO Submitting 1 missing tasks from ResultStage 6 (MapPartitionsRDD[23] at show at IcebergWriter.scala:117) (first 15 tasks are for partitions Vector(0)) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,136] INFO Adding task set 6.0 with 1 tasks resource profile 0 (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:00,137] INFO Starting task 0.0 in stage 6.0 (TID 10) (hadoop, executor 1, partition 0, PROCESS_LOCAL, 4368 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:00,166] INFO Added broadcast_15_piece0 in memory on hadoop:46193 (size: 15.4 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:00,186] INFO Finished task 0.0 in stage 6.0 (TID 10) in 49 ms on hadoop (executor 1) (1/1) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:00,186] INFO Removed TaskSet 6.0, whose tasks have all completed, from pool  (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:00,186] INFO ResultStage 6 (show at IcebergWriter.scala:117) finished in 0.087 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,186] INFO Job 5 is finished. Cancelling potential speculative or zombie tasks for this job (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,186] INFO Killing all running tasks in stage 6: Stage finished (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:00,186] INFO Job 5 finished: show at IcebergWriter.scala:117, took 0.090677 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,190] INFO Starting job: show at IcebergWriter.scala:117 (org.apache.spark.SparkContext)
[2022-03-04 21:51:00,190] INFO Got job 6 (show at IcebergWriter.scala:117) with 2 output partitions (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,190] INFO Final stage: ResultStage 7 (show at IcebergWriter.scala:117) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,190] INFO Parents of final stage: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,191] INFO Missing parents: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,192] INFO Submitting ResultStage 7 (MapPartitionsRDD[23] at show at IcebergWriter.scala:117), which has no missing parents (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,195] INFO Block broadcast_16 stored as values in memory (estimated size 53.3 KiB, free 365.3 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,196] INFO Block broadcast_16_piece0 stored as bytes in memory (estimated size 15.4 KiB, free 365.3 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,197] INFO Added broadcast_16_piece0 in memory on hadoop:40849 (size: 15.4 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:00,197] INFO Created broadcast 16 from broadcast at DAGScheduler.scala:1478 (org.apache.spark.SparkContext)
[2022-03-04 21:51:00,197] INFO Submitting 2 missing tasks from ResultStage 7 (MapPartitionsRDD[23] at show at IcebergWriter.scala:117) (first 15 tasks are for partitions Vector(1, 2)) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,197] INFO Adding task set 7.0 with 2 tasks resource profile 0 (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:00,198] INFO Starting task 0.0 in stage 7.0 (TID 11) (hadoop, executor 1, partition 1, PROCESS_LOCAL, 4368 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:00,206] INFO Added broadcast_16_piece0 in memory on hadoop:46193 (size: 15.4 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:00,258] INFO Starting task 1.0 in stage 7.0 (TID 12) (hadoop, executor 1, partition 2, PROCESS_LOCAL, 4368 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:00,259] INFO Finished task 0.0 in stage 7.0 (TID 11) in 61 ms on hadoop (executor 1) (1/2) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:00,285] INFO Finished task 1.0 in stage 7.0 (TID 12) in 27 ms on hadoop (executor 1) (2/2) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:00,286] INFO Removed TaskSet 7.0, whose tasks have all completed, from pool  (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:00,294] INFO ResultStage 7 (show at IcebergWriter.scala:117) finished in 0.101 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,294] INFO Job 6 is finished. Cancelling potential speculative or zombie tasks for this job (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,294] INFO Killing all running tasks in stage 7: Stage finished (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:00,294] INFO Job 6 finished: show at IcebergWriter.scala:117, took 0.104084 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,319] INFO Code generated in 20.425667 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:51:00,346] INFO Table loaded by catalog: hadoop.db_gb18030_test.tbl_test_1 (org.apache.iceberg.BaseMetastoreCatalog)
[2022-03-04 21:51:00,346] INFO Hadoop table is early exist, ignore create table  (org.apache.iceberg.streaming.utils.HadoopUtils)
[2022-03-04 21:51:00,413] INFO 
Pushing operators to hadoop.db_gb18030_test.tbl_test_1
Pushed filters: 
Filters that were not pushed: 
Output: _src_name#462, _src_db#463, _src_table#464, _src_ts_ms#465L, _src_server_id#466L, _src_file#467, _src_pos#468L, _src_op#469, _src_ts_ms_r#470L, _tsc_id#471, _tsc_total_order#472L, _tsc_data_collection_order#473L, _kfk_topic#474, _kfk_partition#475, _kfk_offset#476L, _kfk_timestamp#477L, id1#478, id2#479, c1#480, c2#481, create_time#482L, update_time#483L, _file#486
          (org.apache.spark.sql.execution.datasources.v2.RowLevelCommandScanRelationPushDown)
[2022-03-04 21:51:00,431] INFO Block broadcast_17 stored as values in memory (estimated size 164.8 KiB, free 365.1 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,432] INFO Block broadcast_17_piece0 stored as bytes in memory (estimated size 22.1 KiB, free 365.1 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,433] INFO Added broadcast_17_piece0 in memory on hadoop:40849 (size: 22.1 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:00,434] INFO Created broadcast 17 from broadcast at SparkBatchScan.java:139 (org.apache.spark.SparkContext)
[2022-03-04 21:51:00,437] INFO Scanning table hadoop.db_gb18030_test.tbl_test_1 snapshot 1630447149345993327 created at 2022-03-04 13:50:52.252 with filter true (org.apache.iceberg.BaseTableScan)
[2022-03-04 21:51:00,497] INFO Block broadcast_18 stored as values in memory (estimated size 164.8 KiB, free 364.9 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,498] INFO Block broadcast_18_piece0 stored as bytes in memory (estimated size 22.1 KiB, free 364.9 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,498] INFO Added broadcast_18_piece0 in memory on hadoop:40849 (size: 22.1 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:00,499] INFO Created broadcast 18 from broadcast at SparkBatchScan.java:139 (org.apache.spark.SparkContext)
[2022-03-04 21:51:00,514] INFO Block broadcast_19 stored as values in memory (estimated size 164.8 KiB, free 364.7 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,516] INFO Block broadcast_19_piece0 stored as bytes in memory (estimated size 22.1 KiB, free 364.7 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,516] INFO Added broadcast_19_piece0 in memory on hadoop:40849 (size: 22.1 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:00,517] INFO Created broadcast 19 from broadcast at SparkBatchScan.java:139 (org.apache.spark.SparkContext)
[2022-03-04 21:51:00,540] INFO Block broadcast_20 stored as values in memory (estimated size 164.8 KiB, free 364.6 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,542] INFO Block broadcast_20_piece0 stored as bytes in memory (estimated size 22.1 KiB, free 364.5 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,542] INFO Added broadcast_20_piece0 in memory on hadoop:40849 (size: 22.1 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:00,543] INFO Created broadcast 20 from broadcast at SparkBatchScan.java:139 (org.apache.spark.SparkContext)
[2022-03-04 21:51:00,558] INFO Block broadcast_21 stored as values in memory (estimated size 164.8 KiB, free 364.4 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,559] INFO Block broadcast_21_piece0 stored as bytes in memory (estimated size 22.1 KiB, free 364.3 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,560] INFO Added broadcast_21_piece0 in memory on hadoop:40849 (size: 22.1 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:00,560] INFO Created broadcast 21 from broadcast at SparkBatchScan.java:139 (org.apache.spark.SparkContext)
[2022-03-04 21:51:00,602] INFO Registering RDD 27 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) as input to shuffle 2 (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,602] INFO Got map stage job 7 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) with 1 output partitions (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,602] INFO Final stage: ShuffleMapStage 8 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,602] INFO Parents of final stage: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,602] INFO Missing parents: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,603] INFO Submitting ShuffleMapStage 8 (MapPartitionsRDD[27] at $anonfun$withThreadLocalCaptured$1 at FutureTask.java:266), which has no missing parents (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,608] INFO Block broadcast_22 stored as values in memory (estimated size 11.8 KiB, free 364.3 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,610] INFO Block broadcast_22_piece0 stored as bytes in memory (estimated size 5.9 KiB, free 364.3 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,610] INFO Added broadcast_22_piece0 in memory on hadoop:40849 (size: 5.9 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:00,611] INFO Created broadcast 22 from broadcast at DAGScheduler.scala:1478 (org.apache.spark.SparkContext)
[2022-03-04 21:51:00,611] INFO Submitting 1 missing tasks from ShuffleMapStage 8 (MapPartitionsRDD[27] at $anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) (first 15 tasks are for partitions Vector(0)) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,611] INFO Adding task set 8.0 with 1 tasks resource profile 0 (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:00,616] INFO Code generated in 7.580666 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:51:00,618] INFO Registering RDD 29 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) as input to shuffle 3 (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,619] INFO Got map stage job 8 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) with 3 output partitions (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,619] INFO Final stage: ShuffleMapStage 9 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,619] INFO Parents of final stage: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,619] INFO Missing parents: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,619] INFO Submitting ShuffleMapStage 9 (MapPartitionsRDD[29] at $anonfun$withThreadLocalCaptured$1 at FutureTask.java:266), which has no missing parents (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,620] INFO Starting task 0.0 in stage 8.0 (TID 13) (hadoop, executor 1, partition 0, NODE_LOCAL, 13013 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:00,627] INFO Block broadcast_23 stored as values in memory (estimated size 48.6 KiB, free 364.3 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,628] INFO Block broadcast_23_piece0 stored as bytes in memory (estimated size 15.0 KiB, free 364.3 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,629] INFO Added broadcast_23_piece0 in memory on hadoop:40849 (size: 15.0 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:00,629] INFO Created broadcast 23 from broadcast at DAGScheduler.scala:1478 (org.apache.spark.SparkContext)
[2022-03-04 21:51:00,629] INFO Submitting 3 missing tasks from ShuffleMapStage 9 (MapPartitionsRDD[29] at $anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) (first 15 tasks are for partitions Vector(0, 1, 2)) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,629] INFO Adding task set 9.0 with 3 tasks resource profile 0 (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:00,639] INFO Added broadcast_22_piece0 in memory on hadoop:46193 (size: 5.9 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:00,655] INFO Added broadcast_20_piece0 in memory on hadoop:46193 (size: 22.1 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:00,788] INFO Starting task 0.0 in stage 9.0 (TID 14) (hadoop, executor 1, partition 0, PROCESS_LOCAL, 4357 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:00,789] INFO Finished task 0.0 in stage 8.0 (TID 13) in 177 ms on hadoop (executor 1) (1/1) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:00,789] INFO Removed TaskSet 8.0, whose tasks have all completed, from pool  (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:00,789] INFO ShuffleMapStage 8 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) finished in 0.186 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,789] INFO looking for newly runnable stages (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,789] INFO running: Set(ShuffleMapStage 9) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,789] INFO waiting: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,789] INFO failed: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,796] INFO Added broadcast_23_piece0 in memory on hadoop:46193 (size: 15.0 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:00,816] INFO Starting task 1.0 in stage 9.0 (TID 15) (hadoop, executor 1, partition 1, PROCESS_LOCAL, 4357 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:00,817] INFO Finished task 0.0 in stage 9.0 (TID 14) in 29 ms on hadoop (executor 1) (1/3) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:00,843] INFO Starting task 2.0 in stage 9.0 (TID 16) (hadoop, executor 1, partition 2, PROCESS_LOCAL, 4357 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:00,844] INFO Finished task 1.0 in stage 9.0 (TID 15) in 29 ms on hadoop (executor 1) (2/3) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:00,868] INFO Finished task 2.0 in stage 9.0 (TID 16) in 25 ms on hadoop (executor 1) (3/3) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:00,868] INFO Removed TaskSet 9.0, whose tasks have all completed, from pool  (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:00,868] INFO ShuffleMapStage 9 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) finished in 0.248 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,868] INFO looking for newly runnable stages (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,868] INFO running: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,868] INFO waiting: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,868] INFO failed: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,878] INFO For shuffle(2, 3), advisory target size: 67108864, actual target size 1048576, minimum partition size: 1048576 (org.apache.spark.sql.execution.adaptive.ShufflePartitionsUtil)
[2022-03-04 21:51:00,882] INFO spark.sql.codegen.aggregate.map.twolevel.enabled is set to true, but current version of codegened fast hashmap does not support this aggregate. (org.apache.spark.sql.execution.aggregate.HashAggregateExec)
[2022-03-04 21:51:00,914] INFO Code generated in 20.375 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:51:00,924] INFO Code generated in 7.882 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:51:00,941] INFO Code generated in 7.241792 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:51:00,962] INFO Registering RDD 36 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) as input to shuffle 4 (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,962] INFO Got map stage job 9 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) with 1 output partitions (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,962] INFO Final stage: ShuffleMapStage 12 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,962] INFO Parents of final stage: List(ShuffleMapStage 10, ShuffleMapStage 11) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,962] INFO Missing parents: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,963] INFO Submitting ShuffleMapStage 12 (MapPartitionsRDD[36] at $anonfun$withThreadLocalCaptured$1 at FutureTask.java:266), which has no missing parents (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,968] INFO Block broadcast_24 stored as values in memory (estimated size 75.9 KiB, free 364.2 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,969] INFO Block broadcast_24_piece0 stored as bytes in memory (estimated size 25.8 KiB, free 364.2 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:00,970] INFO Added broadcast_24_piece0 in memory on hadoop:40849 (size: 25.8 KiB, free: 366.0 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:00,970] INFO Created broadcast 24 from broadcast at DAGScheduler.scala:1478 (org.apache.spark.SparkContext)
[2022-03-04 21:51:00,970] INFO Submitting 1 missing tasks from ShuffleMapStage 12 (MapPartitionsRDD[36] at $anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) (first 15 tasks are for partitions Vector(0)) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:00,970] INFO Adding task set 12.0 with 1 tasks resource profile 0 (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:00,972] INFO Starting task 0.0 in stage 12.0 (TID 17) (hadoop, executor 1, partition 0, NODE_LOCAL, 4713 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:00,980] INFO Added broadcast_24_piece0 in memory on hadoop:46193 (size: 25.8 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,016] INFO Asked to send map output locations for shuffle 2 to 192.168.100.60:49536 (org.apache.spark.MapOutputTrackerMasterEndpoint)
[2022-03-04 21:51:01,035] INFO Asked to send map output locations for shuffle 3 to 192.168.100.60:49536 (org.apache.spark.MapOutputTrackerMasterEndpoint)
[2022-03-04 21:51:01,052] INFO Removed broadcast_11_piece0 on hadoop:40849 in memory (size: 16.1 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,053] INFO Removed broadcast_11_piece0 on hadoop:46193 in memory (size: 16.1 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,058] INFO Removed broadcast_8_piece0 on hadoop:40849 in memory (size: 86.0 B, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,073] INFO Removed broadcast_22_piece0 on hadoop:46193 in memory (size: 5.9 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,074] INFO Removed broadcast_22_piece0 on hadoop:40849 in memory (size: 5.9 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,077] INFO Removed broadcast_13_piece0 on hadoop:40849 in memory (size: 27.2 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,080] INFO Removed broadcast_13_piece0 on hadoop:46193 in memory (size: 27.2 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,086] INFO Removed broadcast_15_piece0 on hadoop:46193 in memory (size: 15.4 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,088] INFO Removed broadcast_15_piece0 on hadoop:40849 in memory (size: 15.4 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,099] INFO Removed broadcast_12_piece0 on hadoop:40849 in memory (size: 22.1 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,099] INFO Removed broadcast_12_piece0 on hadoop:46193 in memory (size: 22.1 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,106] INFO Removed broadcast_10_piece0 on hadoop:40849 in memory (size: 14.9 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,108] INFO Removed broadcast_10_piece0 on hadoop:46193 in memory (size: 14.9 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,112] INFO Removed broadcast_16_piece0 on hadoop:40849 in memory (size: 15.4 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,116] INFO Removed broadcast_16_piece0 on hadoop:46193 in memory (size: 15.4 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,120] INFO Removed broadcast_23_piece0 on hadoop:40849 in memory (size: 15.0 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,121] INFO Removed broadcast_23_piece0 on hadoop:46193 in memory (size: 15.0 KiB, free: 366.3 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,139] INFO Finished task 0.0 in stage 12.0 (TID 17) in 168 ms on hadoop (executor 1) (1/1) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:01,139] INFO Removed TaskSet 12.0, whose tasks have all completed, from pool  (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:01,139] INFO ShuffleMapStage 12 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) finished in 0.175 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,139] INFO looking for newly runnable stages (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,140] INFO running: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,140] INFO waiting: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,140] INFO failed: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,142] INFO For shuffle(4), advisory target size: 67108864, actual target size 1048576, minimum partition size: 1048576 (org.apache.spark.sql.execution.adaptive.ShufflePartitionsUtil)
[2022-03-04 21:51:01,143] INFO spark.sql.codegen.aggregate.map.twolevel.enabled is set to true, but current version of codegened fast hashmap does not support this aggregate. (org.apache.spark.sql.execution.aggregate.HashAggregateExec)
[2022-03-04 21:51:01,155] INFO Code generated in 9.691125 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:51:01,168] INFO Starting job: $anonfun$withThreadLocalCaptured$1 at FutureTask.java:266 (org.apache.spark.SparkContext)
[2022-03-04 21:51:01,169] INFO Got job 10 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) with 1 output partitions (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,169] INFO Final stage: ResultStage 16 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,169] INFO Parents of final stage: List(ShuffleMapStage 15) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,169] INFO Missing parents: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,169] INFO Submitting ResultStage 16 (MapPartitionsRDD[39] at $anonfun$withThreadLocalCaptured$1 at FutureTask.java:266), which has no missing parents (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,172] INFO Block broadcast_25 stored as values in memory (estimated size 69.8 KiB, free 364.7 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:01,173] INFO Block broadcast_25_piece0 stored as bytes in memory (estimated size 24.0 KiB, free 364.7 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:01,175] INFO Added broadcast_25_piece0 in memory on hadoop:40849 (size: 24.0 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,175] INFO Created broadcast 25 from broadcast at DAGScheduler.scala:1478 (org.apache.spark.SparkContext)
[2022-03-04 21:51:01,175] INFO Submitting 1 missing tasks from ResultStage 16 (MapPartitionsRDD[39] at $anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) (first 15 tasks are for partitions Vector(0)) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,175] INFO Adding task set 16.0 with 1 tasks resource profile 0 (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:01,176] INFO Starting task 0.0 in stage 16.0 (TID 18) (hadoop, executor 1, partition 0, NODE_LOCAL, 4442 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:01,186] INFO Added broadcast_25_piece0 in memory on hadoop:46193 (size: 24.0 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,191] INFO Asked to send map output locations for shuffle 4 to 192.168.100.60:49536 (org.apache.spark.MapOutputTrackerMasterEndpoint)
[2022-03-04 21:51:01,209] INFO Finished task 0.0 in stage 16.0 (TID 18) in 33 ms on hadoop (executor 1) (1/1) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:01,209] INFO Removed TaskSet 16.0, whose tasks have all completed, from pool  (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:01,210] INFO ResultStage 16 ($anonfun$withThreadLocalCaptured$1 at FutureTask.java:266) finished in 0.039 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,210] INFO Job 10 is finished. Cancelling potential speculative or zombie tasks for this job (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,210] INFO Killing all running tasks in stage 16: Stage finished (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:01,210] INFO Job 10 finished: $anonfun$withThreadLocalCaptured$1 at FutureTask.java:266, took 0.042043 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,214] INFO Block broadcast_26 stored as values in memory (estimated size 464.0 B, free 364.7 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:01,215] INFO Block broadcast_26_piece0 stored as bytes in memory (estimated size 316.0 B, free 364.7 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:01,215] INFO Added broadcast_26_piece0 in memory on hadoop:40849 (size: 316.0 B, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,216] INFO Created broadcast 26 from sql at IcebergWriter.scala:61 (org.apache.spark.SparkContext)
[2022-03-04 21:51:01,217] INFO Block broadcast_27 stored as values in memory (estimated size 164.8 KiB, free 364.6 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:01,219] INFO Block broadcast_27_piece0 stored as bytes in memory (estimated size 22.1 KiB, free 364.5 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:01,219] INFO Added broadcast_27_piece0 in memory on hadoop:40849 (size: 22.1 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,219] INFO Created broadcast 27 from broadcast at SparkBatchScan.java:139 (org.apache.spark.SparkContext)
[2022-03-04 21:51:01,234] INFO Registering RDD 43 (sql at IcebergWriter.scala:61) as input to shuffle 5 (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,234] INFO Got map stage job 11 (sql at IcebergWriter.scala:61) with 1 output partitions (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,234] INFO Final stage: ShuffleMapStage 17 (sql at IcebergWriter.scala:61) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,234] INFO Parents of final stage: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,234] INFO Missing parents: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,234] INFO Submitting ShuffleMapStage 17 (MapPartitionsRDD[43] at sql at IcebergWriter.scala:61), which has no missing parents (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,243] INFO Block broadcast_28 stored as values in memory (estimated size 19.5 KiB, free 364.5 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:01,244] INFO Block broadcast_28_piece0 stored as bytes in memory (estimated size 7.5 KiB, free 364.5 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:01,244] INFO Added broadcast_28_piece0 in memory on hadoop:40849 (size: 7.5 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,244] INFO Created broadcast 28 from broadcast at DAGScheduler.scala:1478 (org.apache.spark.SparkContext)
[2022-03-04 21:51:01,245] INFO Submitting 1 missing tasks from ShuffleMapStage 17 (MapPartitionsRDD[43] at sql at IcebergWriter.scala:61) (first 15 tasks are for partitions Vector(0)) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,245] INFO Adding task set 17.0 with 1 tasks resource profile 0 (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:01,246] INFO Starting task 0.0 in stage 17.0 (TID 19) (hadoop, executor 1, partition 0, NODE_LOCAL, 12687 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:01,255] INFO Code generated in 11.46075 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:51:01,258] INFO Registering RDD 45 (sql at IcebergWriter.scala:61) as input to shuffle 6 (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,259] INFO Got map stage job 12 (sql at IcebergWriter.scala:61) with 3 output partitions (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,259] INFO Final stage: ShuffleMapStage 18 (sql at IcebergWriter.scala:61) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,259] INFO Parents of final stage: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,259] INFO Missing parents: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,259] INFO Submitting ShuffleMapStage 18 (MapPartitionsRDD[45] at sql at IcebergWriter.scala:61), which has no missing parents (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,264] INFO Block broadcast_29 stored as values in memory (estimated size 54.4 KiB, free 364.5 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:01,264] INFO Block broadcast_29_piece0 stored as bytes in memory (estimated size 16.3 KiB, free 364.4 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:01,265] INFO Added broadcast_28_piece0 in memory on hadoop:46193 (size: 7.5 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,265] INFO Added broadcast_29_piece0 in memory on hadoop:40849 (size: 16.3 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,265] INFO Created broadcast 29 from broadcast at DAGScheduler.scala:1478 (org.apache.spark.SparkContext)
[2022-03-04 21:51:01,266] INFO Submitting 3 missing tasks from ShuffleMapStage 18 (MapPartitionsRDD[45] at sql at IcebergWriter.scala:61) (first 15 tasks are for partitions Vector(0, 1, 2)) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,266] INFO Adding task set 18.0 with 3 tasks resource profile 0 (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:01,274] INFO Added broadcast_27_piece0 in memory on hadoop:46193 (size: 22.1 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,335] INFO Starting task 0.0 in stage 18.0 (TID 20) (hadoop, executor 1, partition 0, PROCESS_LOCAL, 4357 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:01,335] INFO Finished task 0.0 in stage 17.0 (TID 19) in 90 ms on hadoop (executor 1) (1/1) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:01,335] INFO Removed TaskSet 17.0, whose tasks have all completed, from pool  (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:01,336] INFO ShuffleMapStage 17 (sql at IcebergWriter.scala:61) finished in 0.101 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,336] INFO looking for newly runnable stages (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,336] INFO running: Set(ShuffleMapStage 18) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,336] INFO waiting: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,336] INFO failed: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,354] INFO Added broadcast_29_piece0 in memory on hadoop:46193 (size: 16.3 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,381] INFO Starting task 1.0 in stage 18.0 (TID 21) (hadoop, executor 1, partition 1, PROCESS_LOCAL, 4357 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:01,382] INFO Finished task 0.0 in stage 18.0 (TID 20) in 48 ms on hadoop (executor 1) (1/3) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:01,407] INFO Starting task 2.0 in stage 18.0 (TID 22) (hadoop, executor 1, partition 2, PROCESS_LOCAL, 4357 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:01,407] INFO Finished task 1.0 in stage 18.0 (TID 21) in 26 ms on hadoop (executor 1) (2/3) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:01,435] INFO Finished task 2.0 in stage 18.0 (TID 22) in 28 ms on hadoop (executor 1) (3/3) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:01,435] INFO Removed TaskSet 18.0, whose tasks have all completed, from pool  (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:01,436] INFO ShuffleMapStage 18 (sql at IcebergWriter.scala:61) finished in 0.175 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,436] INFO looking for newly runnable stages (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,436] INFO running: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,436] INFO waiting: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,436] INFO failed: Set() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,439] INFO For shuffle(5, 6), advisory target size: 67108864, actual target size 1048576, minimum partition size: 1048576 (org.apache.spark.sql.execution.adaptive.ShufflePartitionsUtil)
[2022-03-04 21:51:01,467] INFO Code generated in 12.99725 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:51:01,478] INFO Code generated in 8.748709 ms (org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator)
[2022-03-04 21:51:01,510] INFO Block broadcast_30 stored as values in memory (estimated size 164.8 KiB, free 364.3 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:01,511] INFO Block broadcast_30_piece0 stored as bytes in memory (estimated size 22.1 KiB, free 364.3 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:01,511] INFO Added broadcast_30_piece0 in memory on hadoop:40849 (size: 22.1 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,512] INFO Created broadcast 30 from broadcast at SparkWrite.java:173 (org.apache.spark.SparkContext)
[2022-03-04 21:51:01,512] INFO Start processing data source write support: IcebergBatchWrite(table=hadoop.db_gb18030_test.tbl_test_1, format=PARQUET). The input RDD has 1 partitions. (org.apache.spark.sql.execution.datasources.v2.ReplaceDataExec)
[2022-03-04 21:51:01,513] INFO Starting job: sql at IcebergWriter.scala:61 (org.apache.spark.SparkContext)
[2022-03-04 21:51:01,513] INFO Got job 13 (sql at IcebergWriter.scala:61) with 1 output partitions (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,513] INFO Final stage: ResultStage 21 (sql at IcebergWriter.scala:61) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,513] INFO Parents of final stage: List(ShuffleMapStage 19, ShuffleMapStage 20) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,513] INFO Missing parents: List() (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,514] INFO Submitting ResultStage 21 (MapPartitionsRDD[53] at sql at IcebergWriter.scala:61), which has no missing parents (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,520] INFO Block broadcast_31 stored as values in memory (estimated size 110.3 KiB, free 364.1 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:01,521] INFO Block broadcast_31_piece0 stored as bytes in memory (estimated size 35.6 KiB, free 364.1 MiB) (org.apache.spark.storage.memory.MemoryStore)
[2022-03-04 21:51:01,521] INFO Added broadcast_31_piece0 in memory on hadoop:40849 (size: 35.6 KiB, free: 366.0 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,522] INFO Created broadcast 31 from broadcast at DAGScheduler.scala:1478 (org.apache.spark.SparkContext)
[2022-03-04 21:51:01,522] INFO Submitting 1 missing tasks from ResultStage 21 (MapPartitionsRDD[53] at sql at IcebergWriter.scala:61) (first 15 tasks are for partitions Vector(0)) (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:01,522] INFO Adding task set 21.0 with 1 tasks resource profile 0 (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:01,523] INFO Starting task 0.0 in stage 21.0 (TID 23) (hadoop, executor 1, partition 0, NODE_LOCAL, 4724 bytes) taskResourceAssignments Map() (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:01,531] INFO Added broadcast_31_piece0 in memory on hadoop:46193 (size: 35.6 KiB, free: 366.2 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:01,561] INFO Asked to send map output locations for shuffle 5 to 192.168.100.60:49536 (org.apache.spark.MapOutputTrackerMasterEndpoint)
[2022-03-04 21:51:01,567] INFO Asked to send map output locations for shuffle 6 to 192.168.100.60:49536 (org.apache.spark.MapOutputTrackerMasterEndpoint)
[2022-03-04 21:51:01,642] INFO Added broadcast_30_piece0 in memory on hadoop:46193 (size: 22.1 KiB, free: 366.1 MiB) (org.apache.spark.storage.BlockManagerInfo)
[2022-03-04 21:51:02,114] INFO Finished task 0.0 in stage 21.0 (TID 23) in 591 ms on hadoop (executor 1) (1/1) (org.apache.spark.scheduler.TaskSetManager)
[2022-03-04 21:51:02,114] INFO Removed TaskSet 21.0, whose tasks have all completed, from pool  (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:02,115] INFO ResultStage 21 (sql at IcebergWriter.scala:61) finished in 0.599 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:02,115] INFO Job 13 is finished. Cancelling potential speculative or zombie tasks for this job (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:02,115] INFO Killing all running tasks in stage 21: Stage finished (org.apache.spark.scheduler.cluster.YarnClusterScheduler)
[2022-03-04 21:51:02,115] INFO Job 13 finished: sql at IcebergWriter.scala:61, took 0.602291 s (org.apache.spark.scheduler.DAGScheduler)
[2022-03-04 21:51:02,115] INFO Data source write support IcebergBatchWrite(table=hadoop.db_gb18030_test.tbl_test_1, format=PARQUET) is committing. (org.apache.spark.sql.execution.datasources.v2.ReplaceDataExec)
[2022-03-04 21:51:02,118] INFO Committing overwrite of 2 data files with 2 new data files, scanSnapshotId: 1630447149345993327, conflictDetectionFilter: true to table hadoop.db_gb18030_test.tbl_test_1 (org.apache.iceberg.spark.source.SparkWrite)
[2022-03-04 21:51:03,082] INFO Committed snapshot 8697118033453020730 (BaseOverwriteFiles) (org.apache.iceberg.SnapshotProducer)
[2022-03-04 21:51:03,100] INFO Committed in 982 ms (org.apache.iceberg.spark.source.SparkWrite)
[2022-03-04 21:51:03,100] INFO Data source write support IcebergBatchWrite(table=hadoop.db_gb18030_test.tbl_test_1, format=PARQUET) committed. (org.apache.spark.sql.execution.datasources.v2.ReplaceDataExec)
[2022-03-04 21:51:03,100] INFO finished write data into hadoop.db_gb18030_test.tbl_test_1 ... (org.apache.iceberg.streaming.write.IcebergWriter)
[2022-03-04 21:51:03,100] INFO commit offset rangers: [OffsetRange(topic: 'test.db_gb18030_test.tbl_test_1', partition: 0, range: [0 -> 0]),OffsetRange(topic: 'test.db_gb18030_test.tbl_test_1', partition: 1, range: [3 -> 4]),OffsetRange(topic: 'test.db_gb18030_test.tbl_test_1', partition: 2, range: [4 -> 10])] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:51:03,100] INFO ----------------------------------------------------------------------------------
 (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:51:03,100] INFO after write accumulator value [
        StatusAccumulator(
            partitionOffsets: Map(
                test.db_gb18030_test.tbl_test_1:0 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 0, range: [0 -> 4], curOffset: 0),
                test.db_gb18030_test.tbl_test_1:1 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 1, range: [3 -> 4], curOffset: 4),
                test.db_gb18030_test.tbl_test_1:2 -> PartitionOffset(topic: 'test.db_gb18030_test.tbl_test_1', partition: 2, range: [4 -> 18], curOffset: 10)
                ),
            schemaVersion: 2
        )
] (org.apache.iceberg.streaming.Kafka2Iceberg)
[2022-03-04 21:51:03,101] INFO Get next batch min schema version by StatusAccumulator cached current offset [OffsetRange(topic: 'test.db_gb18030_test.tbl_test_1', partition: 0, range: [0 -> 0]), OffsetRange(topic: 'test.db_gb18030_test.tbl_test_1', partition: 1, range: [3 -> 4]), OffsetRange(topic: 'test.db_gb18030_test.tbl_test_1', partition: 2, range: [4 -> 10])] (org.apache.iceberg.streaming.utils.SchemaUtils)
[2022-03-04 21:51:03,101] INFO ConsumerConfig values: 
```
