package com.techwolf.poseidon.demo.flink.flinksimple;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.OutputTag;
import com.techwolf.poseidon.demo.flink.flinksimple.entity.AlertEvent;

import java.util.Arrays;
import java.util.Properties;

public class UnionStreamingJob {
    private static final OutputTag<AlertEvent> middleware = new OutputTag<AlertEvent>("MIDDLEWARE") {
    };
    private static final OutputTag<AlertEvent> machine = new OutputTag<AlertEvent>("MACHINE") {
    };
    private static final OutputTag<AlertEvent> docker = new OutputTag<AlertEvent>("DOCKER") {
    };
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

        Properties kafkaSourceProps2 = new Properties();
        kafkaSourceProps2.put("group.id", "test2020");
        kafkaSourceProps2.put("bootstrap.servers", "192.168.1.71:9092");
        kafkaSourceProps2.put("enable.auto.commit", true);
        kafkaSourceProps2.put("max.poll.records", "2000");
        FlinkKafkaConsumer011<String> kafkaSource2 = new FlinkKafkaConsumer011<>(Arrays.asList("test2".split(",")), new SimpleStringSchema(),
                kafkaSourceProps);
        kafkaSource.setStartFromGroupOffsets();
        DataStreamSource<String> text1 = env.addSource(kafkaSource2);
        text.union(text1).print();
        env.execute("Socket Window WordCount");


    }
}
