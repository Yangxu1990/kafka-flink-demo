package com.aaron.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

// 继承Kafka的生产者拦截器接口
public class TimeInterceptor implements ProducerInterceptor<String, String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        // 1、取出本条数据的值
        String value = producerRecord.value();
        // 2、在本条数据前面拼接当前时间戳
        String timestampValue = System.currentTimeMillis() + "," + value;
        // 3、创建ProducerRecord对象并返回拼接好的record。
        //注意：对于传进来的record，只是value变了，topic、分区及key都没改变，
        //所以在构造producerRecord对象的时候，传入的topic等参数还是跟传进来的record保持一致
        ProducerRecord<String, String> newRecord = new ProducerRecord<>(producerRecord.topic(), producerRecord.key(), timestampValue);
        return newRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
