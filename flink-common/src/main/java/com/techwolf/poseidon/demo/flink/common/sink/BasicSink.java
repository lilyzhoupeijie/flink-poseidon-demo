package com.techwolf.poseidon.demo.flink.common.sink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author zhoupeijie
 */
public interface BasicSink <IN> {

    /**
     * @param parameterTool  params for sink/source
     * @param sink the name for sink
     * @return sink
     */
	SinkFunction<IN> getSink(ParameterTool parameterTool, String sink);
}
