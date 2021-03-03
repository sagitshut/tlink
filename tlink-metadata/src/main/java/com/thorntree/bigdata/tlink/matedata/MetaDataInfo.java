package com.thorntree.bigdata.tlink.matedata;

import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.state.api.runtime.SavepointLoader;

/**
 * @Author: liuxiaoshuai
 * @Date: 2021/3/2
 * @Description:
 */
public class MetaDataInfo {

    public static void main(String[] args) throws Exception{
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata("/Users/liuxiaoshuai/vdb1/opt/flink_cp/c4a7ca897dec841a1ece18ef8f47a05f/chk-626/_metadata");
        System.out.println(metadata);
    }
}
