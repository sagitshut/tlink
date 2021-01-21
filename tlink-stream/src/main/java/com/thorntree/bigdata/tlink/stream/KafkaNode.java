package com.thorntree.bigdata.tlink.stream;

/**
 * @Author: liuxiaoshuai
 * @Date: 2021/1/22
 * @Description:
 */
public class KafkaNode {
    public Integer id;
    public String title;
    public String author;
    public String price;
    public String qty;

    public KafkaNode() {
    }

    public KafkaNode(Integer id, String title, String author, String price, String qty) {
        this.id = id;
        this.title = title;
        this.author = author;
        this.price = price;
        this.qty = qty;
    }
}
