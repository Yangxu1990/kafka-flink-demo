package com.aaron;

import com.alibaba.fastjson.parser.JSONToken;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

public class MyConsumer {

    public static void main(String[] args) {
        String kafkaServer = "192.168.1.101:9092,192.168.1.102:9092,192.168.1.104:9092";
        ArrayList<String> topicList = new ArrayList<String>();
        topicList.add("my_topic");

        //消费者配置信息
        Properties properties = new Properties();
        //连接kafka集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        //指定消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-02");
        //重置消费者的offset，从头开始消费。这个参数只在以下两种情况满足之一时才生效
        //1.消费者换组的时候（新创建的组从头开始消费）；2.当前偏移量在broker中不存在（可能数据已经被删除），会从头开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //是否开启kafka内部系统topic（默认情况下，kafka系统主题是不能被消费的）
        //properties.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, true);
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
        //设置消费者消费哪些主题
        consumer.subscribe(topicList);

        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(100));
            for (ConsumerRecord<Integer, String> record : records) {
                System.out.println(record.key() + " " + record.value() + " -> offset:" + record.offset());
            }
            //同步提交，当前线程会阻塞直到offset提交成功
            //consumer.commitSync();
            //异步提交
//            consumer.commitAsync(new OffsetCommitCallback() {
//                @Override
//                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
//                    if (e != null) {
//                        System.out.println("Commit failed for " + offsets);
//                    }
//                }
//            });
        }
    }
}

