package com.flz.kafka.basic;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProduceWithKeyDemo {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        // 通过ProducerConfig指定属性的key，设置服务器地址，kv序列化的方式
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.39.233:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            String key = "id_" + i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("kafka-basic-topic-3", key, "test1123");
            // async
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("send successfully");
                    System.out.println(String.format("key(%s)-partition(%s)-offset(%s)", key, metadata.partition(), metadata.offset()));
                } else {
                    System.out.println("send failed");
                    exception.printStackTrace();
                }
            });
        }
//         sync
        producer.flush();
        producer.close();
        System.out.println("done");
    }
}
