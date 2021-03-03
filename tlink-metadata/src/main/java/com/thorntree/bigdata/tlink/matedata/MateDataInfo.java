package com.thorntree.bigdata.tlink.matedata;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;


/**
 * @Author: liuxiaoshuai
 * @Date: 2021/3/1
 * @Description:
 */
public class MateDataInfo {

    public static void main(String[] args) throws Exception{
        ExecutionEnvironment bEnv   = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(bEnv, "file:////Users/liuxiaoshuai/vdb1/opt/flink_cp/5779318672dd65ee723b5e0cdfab6f2c/chk-2/", new MemoryStateBackend());

        TypeInformation<Tuple2<KafkaTopicPartition, Long>> originalTypeHintTypeInfo =
                new TypeHint<Tuple2<KafkaTopicPartition, Long>>() {}.getTypeInfo();

        DataSet<Tuple2<KafkaTopicPartition, Long>> tuple2DataSet = savepoint.readUnionState("123", "topic-partition-offset-states", originalTypeHintTypeInfo);
        tuple2DataSet.print();
    }

}
