/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.techwolf.poseidon.demo.flink.flinksimple;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

/**
 * ./flink run -m yarn-cluster -ys 1 -ynm flinkOnYarn -yn 1 -yjm 1024 -ytm 1024  -c pers.pj.flink.StreamingJob  /Users/zhoupeijie/Downloads/flink-yarn/target/flink-yarn-1.0-SNAPSHOT.jar
 * @author zhoupeijie
 */
public class WindowStreamingJob {

	public static void main(String[] args) throws Exception {
		//获取执行env
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//从kafka读取数据
		Properties kafkaSourceProps = new Properties();
		kafkaSourceProps.put("group.id", "test2020");
		kafkaSourceProps.put("bootstrap.servers", "192.168.1.71:9092");
		kafkaSourceProps.put("enable.auto.commit", true);
		kafkaSourceProps.put("max.poll.records", "2000");
		FlinkKafkaConsumer011<String> kafkaSource = new FlinkKafkaConsumer011<>(Arrays.asList("test".split(",")), new SimpleStringSchema(),
				kafkaSourceProps);
		kafkaSource.setStartFromGroupOffsets();
		DataStreamSource<String> text = env.addSource(kafkaSource);

		//默认无处理方式,flink会使用processtime来处理timeWindow,

		DataStream<WordWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {

			private static final long serialVersionUID = 6800597108091365154L;

			@Override
			public void flatMap(String value, Collector<WordWithCount> out) {
				for (String word : value.split(",")) {
					out.collect(new WordWithCount(word, 1));
				}
			}
		});
		windowCounts.keyBy("word").timeWindow(Time.seconds(5), Time.seconds(1)).reduce((ReduceFunction<WordWithCount>) (value1, value2) -> new WordWithCount(value1.word, value1.count + value2.count))
				.map(line -> {
					return line;
				});
		windowCounts.keyBy("word").timeWindow(Time.seconds(2)).reduce((ReduceFunction<WordWithCount>) (value1, value2) -> new WordWithCount(value1.word, value1.count + value2.count))
				.map(line -> {
					return line;
				});
		windowCounts.keyBy("word").countWindow(3).reduce((ReduceFunction<WordWithCount>) (value1, value2) -> new WordWithCount(value1.word, value1.count + value2.count))
				.map(line -> {
					return line;
				});
		//不推荐使用滑动的计数窗口
		windowCounts.keyBy("word").countWindow(3, 1).reduce((ReduceFunction<WordWithCount>) (value1, value2) -> new WordWithCount(value1.word, value1.count + value2.count))
				.map(line -> {
					return line;
				});

		//表示如果 5s 内没出现数据则认为超出会话时长，然后计算这个窗口的和
		windowCounts.keyBy("word").window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))).reduce((ReduceFunction<WordWithCount>) (value1, value2) -> new WordWithCount(value1.word, value1.count + value2.count))
				.map(line -> {
					return line;
				});

		env.execute("Socket Window WordCount");


	}
}
