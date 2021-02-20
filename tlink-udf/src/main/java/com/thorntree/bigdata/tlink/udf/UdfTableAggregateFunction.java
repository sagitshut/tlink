package com.thorntree.bigdata.tlink.udf;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

import com.thorntree.bigdata.tlink.udf.UdfAggregateFunction.WeightedAvg;
import com.thorntree.bigdata.tlink.udf.entity.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @description:
 * @author: lxs
 * @create: 2021-02-20 14:44
 */
public class UdfTableAggregateFunction {

    public static class Top2Accum {
        public Double first;
        public Double second;
    }

    @FunctionHint(output = @DataTypeHint("ROW<v Double, rank INT>"))
    public static class Top2 extends TableAggregateFunction<Row, Top2Accum> {

        @Override
        public Top2Accum createAccumulator() {
            Top2Accum acc = new Top2Accum();
            acc.first = Double.MIN_VALUE;
            acc.second = Double.MIN_VALUE;
            return acc;
        }


        public void accumulate(Top2Accum acc, Double v) {
            if (v > acc.first) {
                acc.second = acc.first;
                acc.first = v;
            } else if (v > acc.second) {
                acc.second = v;
            }
        }

        public void merge(Top2Accum acc, java.lang.Iterable<Top2Accum> iterable) {
            for (Top2Accum otherAcc : iterable) {
                accumulate(acc, otherAcc.first);
                accumulate(acc, otherAcc.second);
            }
        }

        public void emitValue(Top2Accum acc, Collector<Row> out) {
            // emit the value and rank
            if (acc.first != Double.MIN_VALUE) {
                out.collect(Row.of(acc.first, 1));
            }
            if (acc.second != Double.MIN_VALUE) {
                out.collect(Row.of(acc.second, 2));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        bsEnv.setParallelism(1);

        //1.读取数据
        DataStreamSource<String> inputStream = bsEnv.readTextFile("/Users/liuxiaoshuai/project/study/study/myworkspace/tlink/tlink-udf/src/main/resources/sensor.txt");

        //2.转化为POJO
        DataStream<SensorReading> map = inputStream.map(line -> {
            String[] split = line.split(",");
            SensorReading sensorReading = new SensorReading();
            sensorReading.setId(split[0]);
            sensorReading.setTimestamp(new Long(split[1]));
            sensorReading.setTemperature(new Double(split[2]));
            return sensorReading;
        });

        //3.将steam转化为table
        Table table = bsTableEnv.fromDataStream(map, $("id"),$("timestamp").as("ts"),$("temperature").as("temp"));

        // 4. 自定义表值函数，实现对id的分割
        //注册自定义表值函数
        bsTableEnv.createTemporarySystemFunction("top2", Top2.class);
        //4.1 在 Table API 里调用注册好的函数
        Table select = table.groupBy($("id"))
            //.flatAggregate("top2(temp) as (v, rank)")
            .flatAggregate(call("top2", $("temp")))
            .select($("id"), $("v"), $("rank"));

        bsTableEnv.toRetractStream(select, Row.class).print("result");

        bsEnv.execute();
    }


}
