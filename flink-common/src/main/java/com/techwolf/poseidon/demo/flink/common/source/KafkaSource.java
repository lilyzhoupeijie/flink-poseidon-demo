package com.techwolf.poseidon.demo.flink.common.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author zhoupeijie
 */
public class KafkaSource implements BasicSource <String>{

    @Override
    public ParallelSourceFunction<String> getSource(ParameterTool parameterTool, String source) {
        Properties kafkaSourceProps = new Properties();
        System.out.println(parameterTool.get(source+".kafka.read.group.id"));
        kafkaSourceProps.put("group.id", parameterTool.get(source+".kafka.read.group.id"));
        kafkaSourceProps.put("bootstrap.servers", parameterTool.get(source+".kafka.read.bootstrap.servers"));
        kafkaSourceProps.put("enable.auto.commit", parameterTool.getBoolean(source+".kafka.read.enable.auto.commit"));
        kafkaSourceProps.put("max.poll.records", "2000");
        FlinkKafkaConsumer011<String> kafkaSource = new FlinkKafkaConsumer011<>(Arrays.asList(parameterTool.get(source+".kafka.read.topics").split(",")), new SimpleStringSchema(),
                kafkaSourceProps);
        kafkaSource.setStartFromGroupOffsets();
        return kafkaSource;
    }
}
