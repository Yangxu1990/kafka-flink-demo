package com.aaron;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class MyBatchWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment
                .createRemoteEnvironment("192.168.1.101", 6123, "/usr/local/flink/flink-1.7.2/examples/batch/WordCount.jar");

        DataSet<String> data = env.readTextFile("hdfs://192.168.1.101:9000/tmp/wordCountFlink.txt");

        data
                .filter(new FilterFunction<String>() {
                    public boolean filter(String value) {
                        return value.startsWith("http://");
                    }
                })
                .writeAsText("hdfs://192.168.1.101:9000/tmp/wordCountFlinkResult.txt");

        env.execute();
    }

}
