
## 创建配置存储表
```SQL
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
COLLATE=utf8mb4_0900_ai_ci;
```


## HDFS 依赖包上传
hive 外表创建需要 `iceberg-hive-runtime-0.13.1.jar` 依赖,参考 `HiveUtils.createOrReplaceHiveTable()` 实现
