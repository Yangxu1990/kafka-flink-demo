package com.aaron;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class MyConsumer {

    public static void main(String[] args) {
        String kafkaServer = "192.168.1.101:9092,192.168.1.102:9092,192.168.1.104:9092";
        ArrayList<String> topicList = new ArrayList<String>();
        topicList.add("topic_20200305");


        Properties properties = new Properties();
        //连接kafka集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        //指定消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
//        properties.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "test-consumer");
        //设置 offset自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //自动提交间隔时间
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //key反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        //value反序列化
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //创建消费者
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(properties);
//        设置消费者消费哪些主题
        consumer.subscribe(topicList);

        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(100));
            for (ConsumerRecord<Integer, String> record : records) {
                System.out.println(record.key() + " " + record.value() + " -> offset:" + record.offset());
            }
        }
    }
}
