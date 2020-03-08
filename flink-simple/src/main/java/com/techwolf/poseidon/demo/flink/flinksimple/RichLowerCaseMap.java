package com.techwolf.poseidon.demo.flink.flinksimple;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * @author zhoupeijie
 * 同样的函数还有RichSinkFunction,RichSourceFunction,RichJoinFunction等等这些
 */
public class RichLowerCaseMap extends RichMapFunction<String,String> {
    @Override
    public void open(Configuration parameters) throws Exception {
        /**
         * 在这里进行初始化的操作
         */
    }
    @Override
    public String map(String line) throws Exception {
        return line.toLowerCase();
    }
}
