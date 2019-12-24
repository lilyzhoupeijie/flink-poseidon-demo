package com.techwolf.poseidon.demo.flink.flinkspringboot;

import com.techwolf.poseidon.demo.flink.common.function.BaseRichMapFunction;
import com.techwolf.poseidon.demo.flink.common.sink.BasicSink;
import com.techwolf.poseidon.demo.flink.common.source.BasicSource;
import com.techwolf.poseidon.demo.flink.common.util.FlinkEnvUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;
import java.util.Objects;

/**
 * @author zhoupeijie
 * from different environment ,from properties
 */
@SpringBootApplication
public class FlinkSpringBootApplicationFromProperties implements CommandLineRunner {
	public static String sourcePackage = "com.techwolf.poseidon.demo.flink.common.source.";
	public static String sinkPackage = "com.techwolf.poseidon.demo.flink.common.sink.";

	public static void main(String[] args) {
		SpringApplication.run(FlinkSpringBootApplicationFromProperties.class, args);
		System.out.println("begin to execute ...");
	}


	@Override
	public void run(String... strings) throws Exception {
		ParameterTool                    parameterTool    = ParameterTool.fromPropertiesFile(new ClassPathResource("application.properties").getFile());
		final StreamExecutionEnvironment env              = FlinkEnvUtil.getEnv(parameterTool);
		DataStreamSource<String>         dataStreamSource = env.addSource(((BasicSource<String>) Class.forName(sourcePackage + parameterTool.get(parameterTool.get("sources") + ".type")).newInstance()).getSource(parameterTool, parameterTool.get("sources")));
		dataStreamSource.print();
		SinkFunction<String> 			 sink 			  = ((BasicSink<String>) Class.forName(sinkPackage + parameterTool.get(parameterTool.get("sinks") + ".type")).newInstance()).getSink(parameterTool, parameterTool.get("sinks"));
		dataStreamSource.map((BaseRichMapFunction) Class.forName(parameterTool.get("functions")).newInstance()).filter(Objects::nonNull).addSink(sink);
		env.execute("test-log");
	}
}

