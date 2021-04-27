package com.thorntree.bigdata.tlink.cep;

import com.alibaba.fastjson.JSON;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @description:
 * @author: lxs
 * @create: 2021-04-26 17:52
 */
public class MyMapSource  extends RichSourceFunction<String> {

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Map<String,Object> map = new HashMap<>();

        map.put("userid","1");
        map.put("time", System.currentTimeMillis());
        map.put("behave","order");
        ctx.collect(JSON.toJSONString(map));
        Thread.sleep(3000);

        map.put("userid","2");
        map.put("time", System.currentTimeMillis());
        map.put("behave","pay");
        ctx.collect(JSON.toJSONString(map));
        Thread.sleep(4000);

        map.put("userid","3");
        map.put("time", System.currentTimeMillis());
        map.put("behave","order");
        ctx.collect(JSON.toJSONString(map));
        Thread.sleep(3000);

        map.put("userid","4");
        map.put("time", System.currentTimeMillis());
        map.put("behave","pay");
        ctx.collect(JSON.toJSONString(map));
        Thread.sleep(4000);

        map.put("userid","5");
        map.put("time", System.currentTimeMillis());
        map.put("behave","order");
        ctx.collect(JSON.toJSONString(map));
        Thread.sleep(1000);

        map.put("userid","6");
        map.put("time", System.currentTimeMillis());
        map.put("behave","pay");
        ctx.collect(JSON.toJSONString(map));
        Thread.sleep(1000);
    }

    @Override
    public void cancel() {

    }
}
