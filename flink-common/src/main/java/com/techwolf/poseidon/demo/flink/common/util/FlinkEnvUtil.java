package com.techwolf.poseidon.demo.flink.common.util;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * 将flink的初始化代码抽取出来
 */
public class FlinkEnvUtil {
	public static StreamExecutionEnvironment getEnv(ParameterTool parameterTool) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(parameterTool);
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		env.enableCheckpointing(100000);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
		env.getCheckpointConfig().setCheckpointTimeout(60000);
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		env.setParallelism(parameterTool.getInt("parallelism", parameterTool.getInt("parallelism",2)));
		env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)));
		return env;
	}
}
