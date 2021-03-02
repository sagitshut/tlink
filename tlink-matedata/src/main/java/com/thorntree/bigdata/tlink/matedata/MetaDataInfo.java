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
        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata("/Users/lxs/mydev/tmp/ck/cd04c2fcbae9d316dcf8dfda919c6c70/chk-3/_metadata");
        System.out.println(metadata);
    }
}
