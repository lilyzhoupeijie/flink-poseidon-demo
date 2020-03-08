package com.techwolf.poseidon.demo.flink.flinksimple.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

public class GZIPFileSource implements SourceFunction<String> {
    private String dataFilePath;

    public GZIPFileSource(String dataFilePath) {
        this.dataFilePath = dataFilePath;
    }

    private Random random = new Random();

    private InputStream    inputStream;
    private BufferedReader reader;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        // 首先要读取 .gz 文件
        inputStream = new GZIPInputStream(new FileInputStream(dataFilePath));
        reader = new BufferedReader(new InputStreamReader(inputStream));
        String line = null;
        while ((line = reader.readLine()) != null) {
            // 模拟发送数据
            TimeUnit.MILLISECONDS.sleep(random.nextInt(500));
            // 发送数据
            ctx.collect(line);
        }
        reader.close();
        reader = null;
        inputStream.close();
        inputStream= null;
    }

    @Override
    public void cancel() { // flink job 被取消的时候，被调用这个方法
        // 关闭资源
        try {
            if (reader != null) {
                reader.close();
            }

            if (inputStream != null) {
                inputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}