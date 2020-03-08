package com.techwolf.poseidon.demo.flink.flinksimple.sink;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import com.techwolf.poseidon.demo.flink.flinksimple.entity.AlertEvent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * @author zhoupeijie
 */
public class MySQLSink extends RichSinkFunction<List<AlertEvent>> {
    PreparedStatement ps;
    BasicDataSource dataSource;
    private Connection connection;
    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getConnection(dataSource);
        String sql = "insert into alert_event(tag,content) values(?, ?);";
        if (connection != null) {
            ps = this.connection.prepareStatement(sql);
        }
    }
    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }
    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<AlertEvent> value, Context context) throws Exception {
        if (ps == null) {
            return;
        }
        for (AlertEvent alertEvent : value) {
            ps.setString(1,alertEvent.getTag() );
            ps.setString(2,alertEvent.getContent());
            ps.addBatch();
        }
        int[] count = ps.executeBatch();//批量后执行
    }

    private static Connection getConnection(BasicDataSource dataSource) {
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://192.168.1.60:3306/flink");
        dataSource.setUsername("root");
        dataSource.setPassword("**");
        //set connection pool params
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);
        Connection con = null;
        try {
            con = dataSource.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return con;
    }
}
