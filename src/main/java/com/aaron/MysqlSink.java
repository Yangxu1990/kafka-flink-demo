package com.aaron;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

//继承Flink的Rich接口，不用每插入一条打开一个MySQL连接
public class MysqlSink extends RichSinkFunction<Tuple3<String, Integer, String>> {
    private Connection connection = null;
    private PreparedStatement preparedStatement = null;
    private String userName = null;
    private String password = null;
    private String driverName = null;
    private String DBUrl = null;

    public MysqlSink(String userName, String passowrd, String DBUrl) {
        this.userName = userName;
        this.password = passowrd;
        this.driverName = "com.mysql.jdbc.Driver";
        this.DBUrl = DBUrl;
    }

    public void invoke(Tuple3<String, Integer, String> value) throws Exception {
        if (connection == null) {
            Class.forName(driverName);
            connection = DriverManager.getConnection(DBUrl, userName, password);
        }
        String sql = "insert into word_count_realtime(word,count, update_time) values(?,?,?)";
//        String sql = "insert into word_count_realtime(word,count) values(?,?)";
        preparedStatement = connection.prepareStatement(sql);

        preparedStatement.setString(1, value.f0);
        preparedStatement.setInt(2, value.f1);
        preparedStatement.setString(3, value.f2);

        preparedStatement.executeUpdate();//返回成功的话就是一个，否则就是0
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(driverName);
        connection = DriverManager.getConnection(DBUrl, userName, password);
    }

    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
