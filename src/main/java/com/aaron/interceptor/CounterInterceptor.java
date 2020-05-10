package com.aaron.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

// 继承Kafka的生产者拦截器接口
public class CounterInterceptor implements ProducerInterceptor<String, String> {
    // 定义两个全局变量用来记录消息的计数
    int successCounter;
    int errorCounter;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        //（重要）这里默认是返回null，会把所有消息过滤掉，所以要改成传入的producerRecord对象
        //因为此拦截器不做任何消息过滤，只是统计条数，所以消息经过此方法后原样返回
        return producerRecord;
    }

    @Override // 发送消息后会有一条回执消息的方法，返回成功或者失败
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        // 定义成功和失败的计数器。消息发送成功，recordMetadata会被赋值
        // 这里也可以通过传入的Exception e是否为空来判断是否发送成功
        if (recordMetadata != null) {
            successCounter++;
        } else {
            errorCounter++;
        }

    }

    @Override
    public void close() {
        // 在生产者关闭的时候打印本次发送成功和失败的条数
        System.out.println("消息发送成功条数：" + successCounter);
        System.out.println("消息发送失败条数：" + errorCounter);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
