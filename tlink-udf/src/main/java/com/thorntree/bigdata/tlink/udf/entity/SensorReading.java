package com.thorntree.bigdata.tlink.udf.entity;

import lombok.Data;

/**
 * @description:
 * @author: lxs
 * @create: 2021-02-20 09:46
 */
@Data
public class SensorReading {
    private String id;
    private Long timestamp;
    private Double temperature;
}
