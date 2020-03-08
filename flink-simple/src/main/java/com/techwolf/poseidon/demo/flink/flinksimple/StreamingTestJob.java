package com.techwolf.poseidon.demo.flink.flinksimple;

import com.techwolf.poseidon.sdk.HiveSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingTestJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String>             dataStreamSource = env.addSource(new HiveSource("select * from pjtest.testHive limit 6000;","testtoken","testtoken","root","http://192.168.1.65:9001"));
        SingleOutputStreamOperator<String[]> hiveSource       = dataStreamSource.map(k -> k.split("\t"));
        hiveSource.print();
        dataStreamSource.print().setParallelism(1);
        env.execute("HiveSourceTest");
    }
}
