package com.thorntree.bigdata.tlink.udf;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

import com.thorntree.bigdata.tlink.udf.entity.SensorReading;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @description:
 * @author: lxs
 * @create: 2021-02-20 09:32
 */
public class UdfScalarFunction {

    public static class HashFunction extends ScalarFunction {
        private int factor = 0;

        @Override
        public void open(FunctionContext context) throws Exception {
            // 获取参数 "hashcode_factor",如果不存在，则使用默认值 "12"
            factor = Integer.parseInt(context.getJobParameter("hashcode_factor", "12"));
        }

        public int eval(String s) {
            if(StringUtils.isBlank(s)){
                return -1;
            }
            return s.hashCode() * factor;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        bsEnv.setParallelism(1);


        bsTableEnv.getConfig().addJobParameter("hashcode_factor", "31");

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

        // 4. 自定义标量函数，实现求id的hash值
        // 4.1 在 Table API 里不经注册直接“内联”调用函数
        Table select = table.select($("id"), $("ts"), call(HashFunction.class, $("id")));

        //注册自定义标量函数
        bsTableEnv.createTemporarySystemFunction("HashFunction",HashFunction.class);
        // 4.2 在 Table API 里调用注册好的函数
        Table resultTable = table.select($("id"), $("ts"),call("HashFunction", $("id")));

        //4.3 在 SQL 里调用注册好的函数
        bsTableEnv.createTemporaryView("sensor",table);
        Table resultSqlTable = bsTableEnv.sqlQuery("select id, ts, HashFunction(id) from sensor");

        bsTableEnv.toAppendStream(select, Row.class).print("select");
        bsTableEnv.toAppendStream(resultTable, Row.class).print("result");
        bsTableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        bsEnv.execute();

    }


}
