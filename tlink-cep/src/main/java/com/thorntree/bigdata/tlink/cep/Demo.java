package com.thorntree.bigdata.tlink.cep;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @description:
 * @author: lxs
 * @create: 2021-04-26 17:51
 */
@Slf4j
public class Demo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         *  接收source并将数据转换成一个tuple
         */
        DataStream<Tuple3<String, Long, String>> myDataStream  = env.addSource(new MyMapSource()).
            map(new MapFunction<String, Tuple3<String, Long, String>>(){
                @Override
                public Tuple3<String, Long, String> map(String value) throws Exception {
                    JSONObject json = JSON.parseObject(value);
                    return new Tuple3(json.getString("userid"),json.getLong("time"),json.getString("behave"));
                }
            }).assignTimestampsAndWatermarks(
            //设置水位线的允许延迟时间为5s
            WatermarkStrategy.<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, String>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, String> element, long recordTimestamp) {
                        return element.f1;
                    }
                })
        );

        /**
         * 定义一个规则
         * 接受到behave是order以后，下一个动作必须是pay才算符合这个需求
         */
        Pattern<Tuple3<String, Long, String>, Tuple3<String, Long, String>> myPattern =
            Pattern.<Tuple3<String, Long, String>>begin("start").where(new IterativeCondition<Tuple3<String, Long, String>>() {
                @Override
                public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {
                    //  System.out.println("value:" + value);
                    return value.f2.equals("order");
                }
            }).next("next").where(new IterativeCondition<Tuple3<String, Long, String>>() {
                @Override
                public boolean filter(Tuple3<String, Long, String> value, Context<Tuple3<String, Long, String>> ctx) throws Exception {
                    return value.f2.equals("pay");
                }
            });
//        myPattern.times(2,4);


        PatternStream<Tuple3<String, Long, String>> pattern = CEP.pattern(myDataStream, myPattern);

        //记录超时的订单
        OutputTag<String> outputTag = new OutputTag<String>("myOutput"){};

        SingleOutputStreamOperator<String> resultStream = pattern.select(outputTag,
            /**
             * 超时的,符合要求但是超出时间限制的数据
             */
            (PatternTimeoutFunction<Tuple3<String, Long, String>, String>) (pattern1, timeoutTimestamp) -> {
                log.error("pattern:"+ pattern1);
                List<Tuple3<String, Long, String>> startList = pattern1.get("start");
                Tuple3<String, Long, String> tuple3 = startList.get(0);
                return tuple3.toString() + "迟到，时间为："+timeoutTimestamp;
            }, new PatternSelectFunction<Tuple3<String, Long, String>, String>() {
                @Override
                public String select(Map<String, List<Tuple3<String, Long, String>>> pattern) throws Exception {
                    //匹配上第一个条件的
                    List<Tuple3<String, Long, String>> startList = pattern.get("start");
                    //匹配上第二个条件的
                    List<Tuple3<String, Long, String>> endList = pattern.get("next");

                    Tuple3<String, Long, String> tuple3 = endList.get(0);
                    return tuple3.toString();
                }
            }
        );

        //输出匹配上规则的数据
        resultStream.print("正常数据");

        //旁路输出超时数据的流
        DataStream<String> sideOutput = resultStream.getSideOutput(outputTag);
        sideOutput.print("超时数据");

        env.execute("Test CEP");
    }
}
