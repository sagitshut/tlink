package com.thorntree.bigdata.tlink.udf;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

import com.thorntree.bigdata.tlink.udf.entity.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @description:
 * @author: lxs
 * @create: 2021-02-20 11:43
 */
public class UdfTableFunction {

    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class SplitFunction extends TableFunction<Row> {

        public void eval(String str) {
            for (String s : str.split("_")) {
                // use collect(...) to emit a row
                collect(Row.of(s, s.length()));
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
        // 4.1 在 Table API 里不经注册直接“内联”调用函数
        Table select = table.joinLateral(call(SplitFunction.class, $("id")))
            .select($("id"), $("ts"), $("word"), $("length"));


        //注册自定义表值函数
        bsTableEnv.createTemporarySystemFunction("SplitFunction",SplitFunction.class);
        // 4.2 在 Table API 里调用注册好的函数
        Table resultTable = table
            .joinLateral(call("SplitFunction", $("id")))
            .select($("id"), $("ts"), $("word"), $("length"));

        //4.3 在 SQL 里调用注册好的函数
        bsTableEnv.createTemporaryView("sensor",table);
        Table resultSqlTable = bsTableEnv.sqlQuery("select id, ts,  word, length  from sensor , LATERAL TABLE(SplitFunction(id))");

        bsTableEnv.toAppendStream(select, Row.class).print("select");
        bsTableEnv.toAppendStream(resultTable, Row.class).print("result");
        bsTableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        bsEnv.execute();

    }

}
