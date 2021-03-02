package com.thorntree.bigdata.tlink.stream;

/**
 * @Author: liuxiaoshuai
 * @Date: 2021/1/22
 * @Description:
 */
public class KafkaNode {
    public Integer id;
    public String name;

    public KafkaNode() {
    }

    public KafkaNode(Integer id, String name) {
        this.id = id;
        this.name = name;
    }
}
