package com.flz.kafka.basic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ConsumerDemo {
    public static void main(String[] args) throws InterruptedException {
        String groupId = "consumer-1-test-group";
        String topic = "kafka-basic-topic-3";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.39.233:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // 从offset起始位置开始读消息
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 订阅topic
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            System.out.println("polling...");
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000L));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(String.format("topic(%s)-key(%s)-partition(%s)-offset(%s)-value(%s)", consumerRecord.topic(),
                        consumerRecord.key(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.value()));
            }
            TimeUnit.SECONDS.sleep(1);
        }
    }
}
