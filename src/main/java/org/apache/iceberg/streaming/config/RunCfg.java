package org.apache.iceberg.streaming.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** RunConfig. */
public class RunCfg implements java.io.Serializable {

    private Properties cfg;

    public static final String  RUN_ENV_DEFAULT = "product";

    /* Spark Config. */
    public static final String SPARK_MASTER = "spark.master";
    public static final String SPARK_APP_NAME = "spark.app.name";

    public static final String SPARK_STREAMING_KAFKA_MAX_RATE_PER_PARTITION = "spark.streaming.kafka.maxRatePerPartition";

    public static final String SPARK_YARN_JARS = "spark.yarn.jars";
    public static final String SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE = "spark.sql.sources.partitionOverwriteMode";
    public static final String SPARK_SQL_EXTENSIONS = "spark.sql.extensions";
    public static final String SPARK_SQL_CATALOG_HADOOP = "spark.sql.catalog.hadoop";
    public static final String SPARK_SQL_CATALOG_HADOOP_TYPE = "spark.sql.catalog.hadoop.type";
    public static final String SPARK_SQL_CATALOG_HADOOP_WAREHOUSE = "spark.sql.catalog.hadoop.warehouse";
    public static final String SPARK_SQL_WAREHOUSE_DIR = "spark.sql.warehouse.dir";

    /* Schema Config. */
    public static final String AVRO_SCHEMA_REGISTRY_VERSION = "avro.schema.registry.version";
    public static final String AVRO_SCHEMA_MERGED_STRUCT_STRING = "avro.schema.merged.struct.string";
    public static final String AVRO_SCHEMA_MERGED_STRING = "avro.schema.merged.string";


    /* Hive Config */
    public static final String HIVE_JDBC_URL = "hive.jdbc.url";
    public static final String HIVE_JDBC_USER = "hive.jdbc.user";
    public static final String HIVE_JDBC_PASSWORD = "hive.jdbc.password";
    public static final String HIVE_EXTEND_JARS = "hive.external.jar";




    /* Database Config. */
    public static final String DATABASE_URL = "database.url";
    public static final String DATABASE_DRIVER = "database.driver";
    public static final String DATABASE_LOGIN_USER = "database.login.user";
    public static final String DATABASE_LOGIN_PASSWORD = "database.login.password";

    /* Table Config. */
    public static final String TABLE_DATABASE_SCHEMA_NAME = "table.database.schema.name";
    public static final String TABLE_DATABASE_TABLE_NAME = "table.database.table.name";
    public static final String TABLE_PRIMARY_KEYS = "table.primary.keys";
    public static final String TABLE_READ_SELECT_COLUMNS = "table.read.select.columns";
    public static final String TABLE_READ_IGNORE_COLUMNS = "table.read.ignore.columns";
    public static final String TABLE_READ_ADD_COLUMNS = "table.read.add.columns";
    public static final String TABLE_READ_FETCH_SIZE = "table.read.fetch.size";
    public static final String TABLE_READ_PARTITION_COLUMN = "table.read.partition.column";
    public static final String TABLE_READ_PARTITION_BOUND_LOWER = "table.read.partition.bound.lower";
    public static final String TABLE_READ_PARTITION_BOUND_UPPER = "table.read.partition.bound.upper";
    public static final String TABLE_READ_PARTITION_NUMBER = "table.read.partition.number";
    public static final String TABLE_READ_WHERE_CONDITION = "table.read.where.condition";

    /* Kafka Config. */
    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    public static final String KAFKA_SCHEMA_REGISTRY_URL = "kafka.schema.registry.url";
    public static final String KAFKA_CONSUMER_GROUP_ID = "kafka.consumer.group.id";
    public static final String KAFKA_CONSUMER_TOPIC = "kafka.consumer.topic";
    public static final String KAFKA_CONSUMER_MAX_POLL_RECORDS = "kafka.consumer.max.poll.records";
    public static final String KAFKA_CONSUMER_KEY_DESERIALIZER = "kafka.consumer.key.deserializer";
    public static final String KAFKA_CONSUMER_VALUE_DESERIALIZER = "kafka.consumer.value.deserializer";
    public static final String KAFKA_AUTO_OFFSET_RESET = "kafka.auto.offset.reset";

    public static final String KAFKA_CONSUMER_COMMIT_TIMEOUT_MILLIS = "kafka.consumer.commit.timeout.millis";


    /*  Metadata  Record & Kafka  */
    public static final String RECORD_METADATA_SOURCE_COLUMNS = "record.metadata.source.columns";
    public static final String RECORD_METADATA_SOURCE_PREFIX = "record.metadata.source.prefix";
    public static final String RECORD_METADATA_TRANSACTION_COLUMNS = "record.metadata.transaction.columns";
    public static final String RECORD_METADATA_TRANSACTION_PREFIX = "record.metadata.transaction.prefix";
    public static final String RECORD_METADATA_KAFKA_COLUMNS = "record.metadata.kafka.columns";
    public static final String RECORD_METADATA_KAFKA_PREFIX = "record.metadata.kafka.prefix";


    /* Iceberg Config. */
    public static final String ICEBERG_TABLE_NAME = "iceberg.table.name";
    public static final String ICEBERG_TABLE_PARTITION_BY = "iceberg.table.partitionBy";
    public static final String ICEBERG_TABLE_LOCATION = "iceberg.table.location";
    public static final String ICEBERG_TABLE_COMMENT = "iceberg.table.comment";
    public static final String ICEBERG_TABLE_PROPERTIES = "iceberg.table.properties";
    public static final String ICEBERG_TABLE_PRIMARY_KEY = "iceberg.table.primaryKey";

    public static final String ICEBERG_MAINTENANCE_ENABLED = "iceberg.maintenance.enabled";
    public static final String ICEBERG_MAINTENANCE_TRIGGERING_TIME = "iceberg.maintenance.triggering.time";
    public static final String ICEBERG_MAINTENANCE_EXECUTE_INTERVAL = "iceberg.maintenance.execute.interval";
    public static final String ICEBERG_MAINTENANCE_SNAPSHOT_EXPIRE_TIME = "iceberg.maintenance.snapshot.expire.time";

    public static final String ICEBERG_MAINTENANCE_COMPACT_FILTER_COLUMN = "iceberg.maintenance.compact.filter.column";
    public static final String ICEBERG_MAINTENANCE_COMPACT_DAY_OFFSET = "iceberg.maintenance.compact.day.offset";
    public static final String ICEBERG_MAINTENANCE_COMPACT_FILE_SIZE_BYTES = "iceberg.maintenance.compact.target.file.size.bytes";
    public static final String ICEBERG_MAINTENANCE_MANIFESTS_FILE_LENGTH = "iceberg.maintenance.manifests.file.length";



    public static RunCfg getInstance() {
        return new RunCfg();
    }

    public Properties loadProperties(Properties properties) {
        Map<String, String> cfgMap = new HashMap<>();
        Set<Map.Entry<Object, Object>> propSet = properties.entrySet();
        for (Map.Entry<Object, Object> prop : propSet) {
            cfgMap.put((String) prop.getKey(), (String) prop.getValue());
        }
        cfg = new Properties();
        cfg.putAll(cfgMap);
        return cfg;
    }

    public Properties getCfg(){
        return this.cfg;
    }


}
