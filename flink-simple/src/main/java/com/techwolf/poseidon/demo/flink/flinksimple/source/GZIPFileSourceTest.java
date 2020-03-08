package com.techwolf.poseidon.demo.flink.flinksimple.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class GZIPFileSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource =
                env.addSource(new GZIPFileSource("/Users/zhoupeijie/Downloads/flink-yarn/src/main/java/pers/pj/flink/nycTaxiFares.gz"));

        dataStreamSource.print().setParallelism(1);

        env.execute("GZIPFileSourceTest");
    }
}
