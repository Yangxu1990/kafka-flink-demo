package com.aaron;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;


import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;
import java.util.Date;

import com.aaron.MysqlSink;


public class MyStreamWordCount {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String kafkaServer = "192.168.1.101:9092,192.168.1.102:9092,192.168.1.104:9092";
        String groupId = "test-consumer";
        String topic = "word_count_topics";
        String mysqlUserName = "root";
        String mysqlPassword = "xxx";
        String DBurl = "jdbc:mysql://192.168.1.102:3306/big_data_project";


        Properties prop = new MyStreamWordCount().getConsumerProp(groupId, kafkaServer);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop);
        consumer.setStartFromEarliest(); // 从最早开始消费

//        根据Kafka消费者构建Flink source
        DataStream<String> stream = env.addSource(consumer);

//        stream.print(); // 将消息原封不动打印至控制台
        DataStream<Tuple3<String, Integer, String>> windowCounts = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                Map maps = (Map) JSON.parse(value); // 解析字符串为json
                String word = (String) maps.get("word"); // 获取word字段的单词
                collector.collect(Tuple2.of(word, 1)); // 将单词进行归一操作
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(10)) // 10秒统计一次
                .sum(1)
                // 增加当前时间字段
                .map(new MapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, String>>() {
                    @Override
                    public Tuple3<String, Integer, String> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        // 获取当前时间
                        String times = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
                        // 返回统计好的词频和当前时间（总共3个字段）
                        return Tuple3.of(stringIntegerTuple2.f0, stringIntegerTuple2.f1, times);
                    }
                });
        // 将窗口结果打印在控制台
        windowCounts.print().setParallelism(1);
        // 实例化MySQL  Sink对象
        MysqlSink mysqlSink = new MysqlSink(mysqlUserName, mysqlPassword, DBurl);
        // 将本窗口的数据插入MySQL
        windowCounts.addSink(mysqlSink);
        env.execute("flink test program");
    }

    //传入消费者群组，kafka集群的地址，返回消费者属性
    public Properties getConsumerProp(String groupId, String kafkaServer) {
        Properties properties;
        properties = new Properties();
        //连接kafka集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        //指定消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        properties.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "test-consumer");
        //设置 offset自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //自动提交间隔时间
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //key反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        //value反序列化
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        return properties;
    }
}
