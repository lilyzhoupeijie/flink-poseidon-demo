package com.techwolf.poseidon.demo.flink.flinksimple;


import com.google.common.collect.Lists;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import com.techwolf.poseidon.demo.flink.flinksimple.entity.AlertEvent;
import com.techwolf.poseidon.demo.flink.flinksimple.sink.MySQLSink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class SplitStreamingJob {
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
        //MIDDLEWARE,this is content
        //DOCKER,this is content
        DataStream<AlertEvent> splitStreams = text.filter(line-> line.split(",").length == 2)
                .map(line->new AlertEvent(line.split(",")[0],line.split(",")[1]))
                .process(new ProcessFunction<AlertEvent, AlertEvent>() {
                    @Override
                    public void processElement(AlertEvent alertEvent, Context context, Collector<AlertEvent> collector) throws Exception {
                        if ("MACHINE".equals(alertEvent.getTag())) {
                            context.output(machine, alertEvent);
                        } else if ("DOCKER".equals(alertEvent.getTag())) {
                            context.output(docker, alertEvent);
                        } else if ("MIDDLEWARE".equals(alertEvent.getTag())) {
                            context.output(middleware, alertEvent);
                        } else {
                            //其他的业务逻辑
                            collector.collect(alertEvent);
                        }
                    }
                });
        ((SingleOutputStreamOperator<AlertEvent>) splitStreams).getSideOutput(machine).print();
        splitStreams.timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction<AlertEvent, List<AlertEvent>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<AlertEvent> values, Collector<List<AlertEvent>> out) throws Exception {
                ArrayList<AlertEvent> alerts = Lists.newArrayList(values);
                if (alerts.size() > 0) {
                    out.collect(alerts);
                }
            }
        }).addSink(new MySQLSink()).setParallelism(1);

        env.execute("Socket Window WordCount");


    }
}
