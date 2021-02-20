package com.thorntree.bigdata.tlink.udf;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

import com.thorntree.bigdata.tlink.udf.entity.SensorReading;
import java.util.Iterator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * @description:
 * @author: lxs
 * @create: 2021-02-20 13:47
 */
public class UdfAggregateFunction {

    public static class WeightedAvgAccum {
        public Double sum = 0.0;
        public Integer count = 0;
    }

    public static class WeightedAvg extends AggregateFunction<Double, WeightedAvgAccum> {

        @Override
        public WeightedAvgAccum createAccumulator() {
            return new WeightedAvgAccum();
        }

        @Override
        public Double getValue(WeightedAvgAccum acc) {
            if (acc.count == 0) {
                return null;
            } else {
                return acc.sum / acc.count;
            }
        }

        public void accumulate(WeightedAvgAccum acc, Double iValue) {
            acc.sum += iValue ;
            acc.count += 1;
        }

        public void retract(WeightedAvgAccum acc, Double iValue) {
            acc.sum -= iValue ;
            acc.count -= 1;
        }

        public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
            Iterator<WeightedAvgAccum> iter = it.iterator();
            while (iter.hasNext()) {
                WeightedAvgAccum a = iter.next();
                acc.count += a.count;
                acc.sum += a.sum;
            }
        }

        public void resetAccumulator(WeightedAvgAccum acc) {
            acc.count = 0;
            acc.sum = 0.00;
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
        bsTableEnv.createTemporarySystemFunction("wAvg",WeightedAvg.class);
        //4.1 在 Table API 里调用注册好的函数
        Table select = table.groupBy($("id"))
            .aggregate(call("wAvg", $("temp")).as("aggav"))
            .select($("id"), $("aggav"));

        //4.2 在 SQL 里调用注册好的函数
        bsTableEnv.createTemporaryView("sensor",table);
        Table resultSqlTable = bsTableEnv.sqlQuery("select id, wAvg(temp) AS avgPoints  from sensor group by id");

        bsTableEnv.toRetractStream(select, Row.class).print("result");
        bsTableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");

        bsEnv.execute();
    }
}
