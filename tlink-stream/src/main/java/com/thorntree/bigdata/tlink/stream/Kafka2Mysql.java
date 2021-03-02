package com.thorntree.bigdata.tlink.stream;


import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;


/**
 * @Author: liuxiaoshuai
 * @Date: 2021/1/21
 * @Description:
 */
public class Kafka2Mysql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度，为了方便测试，查看消息的顺序，这里设置为1，可以更改为多并行度
        env.setParallelism(1);
        //checkpoint设置
        //每隔10s进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(10000);
        //设置模式为：exactly_one，仅一次语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //同一时间只允许进行一次检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置statebackend,将检查点保存在hdfs上面，默认保存在内存中。这里先保存到本地
        env.setStateBackend(new FsStateBackend("file:///Users/lxs/mydev/tmp/ck/"));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.3.100:9092,192.168.3.101:9092,192.168.3.102:9092");
        properties.setProperty("group.id", "group_test_lxs");
        //properties.setProperty("auto.offset.reset","latest");
        //kafka分区自动发现周期
        properties.put(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "3000");


        DataStreamSource<KafkaNode> stream = env
                .addSource(new FlinkKafkaConsumer<>("test_lxs", new KafkaDeserializationSchema<KafkaNode>() {
                    @Override
                    public boolean isEndOfStream(KafkaNode kafkaNode) {
                        return false;
                    }

                    @Override
                    public KafkaNode deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                        String s = new String(consumerRecord.value(), "UTF-8");
                        KafkaNode kafkaNode = JSON.parseObject(s, KafkaNode.class);
                        return kafkaNode;
                    }

                    @Override
                    public TypeInformation<KafkaNode> getProducedType() {
                        return TypeInformation.of(KafkaNode.class);
                    }
                }, properties));
        stream.map(a->a.name).print();
        /*stream.addSink(JdbcSink.sink(
                "insert into canal_log (id, name) values (?,?)",
                (ps, t) -> {
                    ps.setInt(1, t.id);
                    ps.setString(2, t.name);
                },
                JdbcExecutionOptions.builder().withBatchSize(3).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.3.100:13306/kafka_canal?characterEncoding=UTF-8&allowMultiQueries=true")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("r1dd16c1a1980475")
                        .build()));*/
        env.execute();
    }
}
