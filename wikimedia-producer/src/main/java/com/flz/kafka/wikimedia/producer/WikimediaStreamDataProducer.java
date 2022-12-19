package com.flz.kafka.wikimedia.producer;

import com.flz.kafka.wikimedia.event.handler.WikimediaStreamEventHandler;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class WikimediaStreamDataProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        // 通过ProducerConfig指定属性的key，设置服务器地址，kv序列化的方式
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.39.233:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        String topic = "wikimedia-stream-data";
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventHandler eventHandler = new WikimediaStreamEventHandler(producer, topic);
        EventSource eventSource = new EventSource.Builder(eventHandler, URI.create(url))
                .proxy("127.0.0.1", 10809) // wiki国内不能直连
                .build();
        eventSource.start();

        TimeUnit.SECONDS.sleep(15);
        log.info("stop reading data");
    }
}
