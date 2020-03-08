package com.techwolf.poseidon.demo.flink.flinksimple;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class MapPartitionDemoJob {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //产生数据
        DataSet<Long> ds = env.generateSequence(1, 20);
        ((DataSource<Long>) ds).setParallelism(3);

        ds.mapPartition(new MyMapPartitionFunciton()).print();
    }

    public static class MyMapPartitionFunciton extends RichMapPartitionFunction<Long, Long> {
        @Override
        public void open(Configuration parameters) throws Exception {
            /**
             * 比如每拿到一条数据,去mysql中根据这个数据去获取一个新的值,那mysql的初始化建议写在这里
             */
        }
        @Override
        public void mapPartition(Iterable<Long> values, Collector<Long> out) throws Exception {
            long count =0;
            for(Long value:values){
                count++;
            }
            out.collect(count);
        }
    }
}
