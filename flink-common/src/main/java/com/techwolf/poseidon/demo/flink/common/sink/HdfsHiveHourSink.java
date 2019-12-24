package com.techwolf.poseidon.demo.flink.common.sink;

import com.techwolf.poseidon.demo.flink.common.util.DateTimeHourBucketer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;

import java.time.ZoneId;

/**
 * @author zhoupeijie
 */
public class HdfsHiveHourSink implements BasicSink<String>{

    @Override
    public SinkFunction<String> getSink(ParameterTool parameterTool, String sink) {
        String pathFormat="yyyy-MM-dd";
        String zone="Asia/Shanghai";
        String hour="HH";
        String path=parameterTool.get(sink+".hdfs.path");
        BucketingSink<String> hdfsSink=new BucketingSink<String>(path);
        hdfsSink.setBucketer(new DateTimeHourBucketer<String>(pathFormat,hour,ZoneId.of(zone)));
        hdfsSink.setWriter(new StringWriter<String>());
        hdfsSink.setBatchSize(1024*1024*400L);
        hdfsSink.setBatchRolloverInterval(60*1000*20L);
        hdfsSink.setInactiveBucketCheckInterval(60*1000L);
        hdfsSink.setInactiveBucketThreshold(60*1000L);
        return hdfsSink;
    }
}
