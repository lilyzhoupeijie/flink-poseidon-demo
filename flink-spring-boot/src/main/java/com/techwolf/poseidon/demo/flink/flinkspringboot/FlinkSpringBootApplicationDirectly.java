package com.techwolf.poseidon.demo.flink.flinkspringboot;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.util.Properties;

/**
 * @author zhoupeijie
 * diectly start source and sink
 */
@SpringBootApplication
public class FlinkSpringBootApplicationDirectly implements CommandLineRunner {
	public static void main(String[] args) {
		SpringApplication.run(FlinkSpringBootApplicationDirectly.class, args);
		System.out.println("begin to execute ...");
	}

	@Override
	public void run(String... strings) throws Exception {
		StreamExecutionEnvironment    env         = StreamExecutionEnvironment.getExecutionEnvironment();
		FlinkKafkaConsumer011<String> kafkaSource = new FlinkKafkaConsumer011<>("web_exposure_time", new SimpleStringSchema(),getProperties());
		DataStream<String>            dataStream  = env.addSource(kafkaSource);
		dataStream.print();
		env.execute();
	}

	private Properties getProperties() {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "172.21.192.82:9092,172.21.192.65:9092,172.21.192.58:9092,172.21.192.148:9092,172.21.192.162:9092,172.21.192.48:9092,172.21.192.158:9092");
		properties.setProperty("group.id", "test-log");
		properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return properties;
	}

}

