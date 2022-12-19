package com.flz.kafka.wikimedia.event.handler;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Optional;

@Slf4j
public class WikimediaStreamEventHandler implements EventHandler {
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public WikimediaStreamEventHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        log.info("stream connection has been opened");
    }

    @Override
    public void onClosed() throws Exception {
        log.info("stream connection has been closed");
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info("receive data, start processing...");
        String data = messageEvent.getData();
        log.info("current received data:{}", data);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, data);
        producer.send(producerRecord, (metadata, exception) -> Optional.ofNullable(exception)
                .ifPresentOrElse((e) -> {
                    log.error("send data to kafka failed:{}", e);
                }, () -> {
                    log.info("successfully send data to kafka");
                }));
    }

    @Override
    public void onComment(String comment) throws Exception {
        log.info("receive comment:{}", comment);
    }

    @Override
    public void onError(Throwable t) {
        log.error("error occurred when reading stream data:{}", t);
    }
}
