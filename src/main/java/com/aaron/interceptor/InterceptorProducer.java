package com.aaron.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

// 带拦截器的生产者
public class InterceptorProducer {
    public static void main(String[] args) {
        // 创建Kafka生产者的配置信息
        Properties properties = new Properties();
        String bootStrapServers = "192.168.1.101:9092, 192.168.1.102:9092, 182.168.1.103:9092";
        // 指定Kafka集群地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        // ACK应答级别
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 消息批次大小：16k
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        // 多长时间发送一次：100毫秒
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        // 缓冲区大小32MB
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        // 指定消息的k,v序列化类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // 添加分区器：对于自定义分区的场景，需要继承Partitioner接口
        // properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.aaron.MyPartitioner");

        // 添加拦截器集合（集合元素是全类名）
        ArrayList<String> interceptors = new ArrayList<>();
        // 注意拦截器添加的顺序，肯定是先拼接时间戳的拦截器，再执行计数拦截器
        interceptors.add("com.aaron.interceptor.TimeInterceptor");
        interceptors.add("com.aaron.interceptor.CounterInterceptor");
        // 将拦截器集合添加到生产者配置里
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        // 创建kafka生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 发送10条数据
        for (int i = 0; i < 100000; i++) {
            // 指定发送到哪个分区，且指定Key的值为key-test
            // ProducerRecord<String, String> record = new ProducerRecord<>("my_topic", 0, "key-test", "this is test message of " + i);
            // 只指定topic名称与发送的数据，默认会通过轮询发送到各个分区里
            ProducerRecord<String, String> record = new ProducerRecord<>("my_topic", "this is test message of " + i);
            // 异步发送
            producer.send(record);
            // 同步发送：只需要再调用get()方法即可。该方法的作用是阻塞线程，生产环境中用得非常少
            // producer.send(record).get();

        }
        // 关闭资源
        producer.close();
    }
}
