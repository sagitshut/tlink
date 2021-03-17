package com.thorntree.bigdata.tlink.file;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: liuxiaoshuai
 * @Date: 2021/3/17
 * @Description:
 */
public class Kafka2ParquetFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);


        String mysqlSourceDdl = "create TABLE kafka_table_test (\n " +
                "  id INT,\n" +
                "  name string,\n" +
                "  description string,\n" +
                "  es  bigint,  \n" +
                "  log_ts AS TO_TIMESTAMP(FROM_UNIXTIME(es, 'yyyy-MM-dd HH:mm:ss')),\n" +
                "  WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND\n" +
                "  ) WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'lxs_test_file',\n" +
                " 'properties.bootstrap.servers' = '192.168.3.102:9092',\n" +
                " 'properties.group.id' = 'lxs_test',\n" +
                " 'scan.startup.mode' = 'group-offsets',\n" +
                " 'format' = 'json',\n" +
                " 'json.fail-on-missing-field' = 'false',\n" +
                " 'json.ignore-parse-errors' = 'true'\n" +
                ")";


        String fileSinkDdl = "CREATE TABLE products_file (\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  description STRING, \n" +
                "  dt string, \n" +
                "  `hour` string "+
                ")  PARTITIONED BY (dt, `hour`) WITH (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='file:///Users/lxs/Documents/test',\n" +
                "  'format'='parquet',\n" +
                "  'sink.partition-commit.delay'='1 s',"+
                "  'sink.partition-commit.policy.kind'='success-file'"+
                ")";


        bsTableEnv.executeSql(mysqlSourceDdl);

        bsTableEnv.executeSql(fileSinkDdl);

        bsTableEnv.executeSql("INSERT INTO products_file SELECT id, name,description, DATE_FORMAT(log_ts, 'yyyy-MM-dd'), DATE_FORMAT(log_ts, 'HH') FROM kafka_table_test");

        bsEnv.execute();
    }
}
