package com.flz.kafka.opensearch.consumer;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class OpenSearchConsumer {
    public static void main(String[] args) throws IOException, InterruptedException {
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        try (openSearchClient) {
            String index = initOpenSearch(openSearchClient);
            KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(5000));
                BulkRequest bulkRequest = new BulkRequest();
                ImmutableList.copyOf(consumerRecords.iterator()).stream()
                        .map(ConsumerRecord::value)
                        .map((value) -> resolveMessage(value, index))
                        .filter(Objects::nonNull)
                        .forEach(bulkRequest::add);
                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("consumer batch message with length {} and save into open search", bulkResponse.getItems().length);
                    kafkaConsumer.commitSync();
                    log.info("offset manual committed");
                }
                TimeUnit.MILLISECONDS.sleep(1500L);
            }
        }
    }

    private static IndexRequest resolveMessage(String value, String index) {
        return Optional.ofNullable(getMessageId(value))
                .map((id) -> new IndexRequest(index)
                        .id(id)
                        .source(value, XContentType.JSON)
                )
                .orElse(null);
    }

    private static String getMessageId(String json) {
        try {
            return JsonParser.parseString(json)
                    .getAsJsonObject()
                    .get("meta")
                    .getAsJsonObject()
                    .get("id")
                    .getAsString();
        } catch (Exception e) {
            return null;
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "wikimedia-opensearch-consumer-group";
        String topic = "wikimedia-stream-data";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.39.233:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    private static String initOpenSearch(RestHighLevelClient client) throws IOException {
        String index = "wikimedia";
        GetIndexRequest getIndexRequest = new GetIndexRequest(index);
        if (!client.indices().exists(getIndexRequest, RequestOptions.DEFAULT)) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
            CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            if (createIndexResponse.isAcknowledged()) {
                log.info("successfully create index '{}'", index);
            }
        } else {
            log.info("index '{}' already existed", index);
        }
        return index;
    }

    private static RestHighLevelClient createOpenSearchClient() {
        return new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.39.233", 9200)));
    }
}
