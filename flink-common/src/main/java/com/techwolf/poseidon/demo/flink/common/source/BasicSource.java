package com.techwolf.poseidon.demo.flink.common.source;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * @author zhoupeijie
 */
public interface BasicSource<OUT>  {

	ParallelSourceFunction<OUT> getSource(ParameterTool parameterTool, String source);
}
