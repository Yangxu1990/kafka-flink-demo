package com.aaron.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

class HBaseTest {
    private static Connection conn;
    private static Admin admin;


    // 静态代码块
    static {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "host1:2181, host2:2181, host3:2181");

        try {
            // 创建hbase连接
            conn = ConnectionFactory.createConnection(config);
            // 创建admin对象
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 参数列表：表名，rowkey，列族，列名，需要插入的值
    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
        // 获取表对象
        Table table = conn.getTable(TableName.valueOf(tableName));
        // 构造put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        // 添加rowkey等信息
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        // 插入数据
        table.put(put);
        // 关闭表资源
        table.close();
    }
}
