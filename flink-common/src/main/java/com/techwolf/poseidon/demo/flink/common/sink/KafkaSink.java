package com.techwolf.poseidon.demo.flink.common.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import java.util.Optional;
import java.util.Properties;

/**
 * @author zhoupeijie
 */
public class KafkaSink implements BasicSink<String>{

    @Override
    public SinkFunction<String> getSink(ParameterTool parameterTool, String sink){
        Properties kafkaSinkProps = new Properties();
        kafkaSinkProps.put("auto.commit.interval.ms","1000");
        kafkaSinkProps.put("bootstrap.servers", parameterTool.get(sink+".kafka.write.bootstrap.servers"));
        kafkaSinkProps.put("client.id", parameterTool.get(sink+".kafka.write.client.id"));
        kafkaSinkProps.put("acks", parameterTool.get(sink+".kafka.write.acks"));
        kafkaSinkProps.put("retries", parameterTool.getInt(sink+".kafka.write.retries"));
        FlinkKafkaProducer011<String> kafkaSink = new FlinkKafkaProducer011<>(parameterTool.get(sink+".kafka.write.topic"), new SimpleStringSchema(), kafkaSinkProps, Optional.empty());
        kafkaSink.setWriteTimestampToKafka(true);
        kafkaSink.setLogFailuresOnly(true);
        return kafkaSink;
    }
}
