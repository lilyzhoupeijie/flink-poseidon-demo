package com.techwolf.poseidon.demo.flink.flinkspringboot.functions;

import com.techwolf.poseidon.demo.flink.common.function.BaseRichMapFunction;

/**
 * @author zhoupeijie
 */
public class PrintMap extends BaseRichMapFunction {
    @Override
    public String map(String line) throws Exception {
        System.out.println(line);
        return line;
    }
}
