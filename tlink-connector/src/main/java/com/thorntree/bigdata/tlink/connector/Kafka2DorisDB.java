package com.thorntree.bigdata.tlink.connector;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @description:
 * @author: lxs
 * @create: 2021-04-12 10:11
 */
public class Kafka2DorisDB {

    public static void main(String[] args) {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(bsSettings);

        //设置并行度，为了方便测试，查看消息的顺序，这里设置为1，可以更改为多并行度
        bsEnv.setParallelism(1);
        //checkpoint设置
        //每隔10s进行启动一个检查点【设置checkpoint的周期】
        bsEnv.enableCheckpointing(10000);
        //设置模式为：exactly_one，仅一次语义
        bsEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
        bsEnv.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
        bsEnv.getCheckpointConfig().setCheckpointTimeout(10000);
        //同一时间只允许进行一次检查点
        bsEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
        bsEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置statebackend,将检查点保存在hdfs上面，默认保存在内存中。这里先保存到本地
        bsEnv.setStateBackend(new FsStateBackend("file:///Users/liuxiaoshuai/vdb1/opt/flink_cp/"));


        String kafkaSource = "CREATE TABLE kafkaDorisDB (\n"
            + "  `id` INT,\n"
            + "  `name` STRING\n"
            + ") WITH (\n"
            + "  'connector' = 'kafka',\n"
            + "  'topic' = 'kafka_dorisdb',\n"
            + "  'properties.bootstrap.servers' = '192.168.3.101:9092',\n"
            + "  'properties.group.id' = 'kafka_dorisdb',\n"
            + "  'format' = 'json'\n"
            + ")";

        tableEnv.executeSql(kafkaSource);

        String dorisDbSink = "CREATE TABLE table_kafka6(" +
            "id INT," +
            "name VARCHAR" +
            ") WITH ( " +
            "'connector' = 'doris'," +
            "'jdbc-url'='jdbc:mysql://192.168.3.102:9030?lxs_db'," +
            "'load-url'='192.168.3.102:8030'," +
            "'database-name' = 'lxs_db'," +
            "'table-name' = 'table_kafka6'," +
            "'username' = 'root'," +
            "'password' = ''," +
            "'sink.buffer-flush.max-rows' = '1000000'," +
            "'sink.buffer-flush.max-bytes' = '300000000'," +
            "'sink.buffer-flush.interval-ms' = '300000'," +
            "'sink.max-retries' = '3'" +
            ")";

        tableEnv.executeSql(dorisDbSink);

        tableEnv.executeSql("insert into table_kafka6 select * from kafkaDorisDB");
    }

}
