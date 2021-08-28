package com.example.helper.service;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.util.Assert;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"customer-topic", "customer-retry-topic","customer-dead-topic"}, partitions = 1)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
  "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
@Slf4j
public class RetryServiceImplTest {
  @Autowired
  private RetryService retryService;

  @Autowired
  private EmbeddedKafkaBroker embeddedKafkaBroker;

  @Value("${kafka-helper.max-retry-count}")
  private Integer maxRetryCount;

  @Autowired
  private KafkaTemplate<String, Customer> kafkaTemplate;


  private String  customerTopic = "customer-topic";

  @Value("${kafka-helper.retry-topic}")
  private String  customerRetryTopic;

  @Value("${kafka-helper.dead-topic}")
  private String  customerDeadTopic;


  private Consumer<String,?> consumer;

  @BeforeEach
  void setUp() {
    Map<String,Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
    consumer = new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), new JsonDeserializer()).createConsumer();
    embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
  }

  @AfterEach
  void tearDown() {
    consumer.close();
  }


  @Test
  @Timeout(5)
  public void testRetryMessageProcessTime() {
    processRecord(10);

    ConsumerRecord<String, ?> consAfterRetry =  KafkaTestUtils.getSingleRecord(consumer,customerRetryTopic);
    Map<String, String> headerMap = getHeaders(consAfterRetry);

    Assert.isTrue(!retryService.processMessage(
      (ConsumerRecord<String, Object>) consAfterRetry,
      headerMap.get("createdTime"),
      headerMap.get("executionTime"),
      headerMap.get("retryCount"),
      headerMap.get("targetTopic")
      ), "Message Processed Immediately");
  }

  @Test
  @Timeout(5)
  public void testRetryMessageToCustomerQueue() {
    processRecord(1);

    ConsumerRecord<String, ?> consAfterRetry =  KafkaTestUtils.getSingleRecord(consumer,customerRetryTopic);
    Map<String, String> headerMap = getHeaders(consAfterRetry);

    Assert.isTrue(retryService.processMessage(
      (ConsumerRecord<String, Object>) consAfterRetry,
      headerMap.get("createdTime"),
      headerMap.get("createdTime"),
      headerMap.get("retryCount"),
      headerMap.get("targetTopic")
    ), "Message not Processed");

    ConsumerRecord<String, ?> consumerRecord =  KafkaTestUtils.getSingleRecord(consumer,customerTopic);
    Assert.isTrue(consumerRecord.key().equals("customer1"), "Customer key not matched");
  }

  @Test
  @Timeout(5)
  public void testRetryDeadQueue() {
    processRecord(1);

    ConsumerRecord<String, ?> consAfterRetry =  KafkaTestUtils.getSingleRecord(consumer,customerRetryTopic);
    Map<String, String> headerMap = getHeaders(consAfterRetry);

    Assert.isTrue(retryService.processMessage(
      (ConsumerRecord<String, Object>) consAfterRetry,
      headerMap.get("createdTime"),
      headerMap.get("createdTime"),
      String.valueOf(maxRetryCount + 1),
      headerMap.get("targetTopic")
    ), "Message not Processed");

    ConsumerRecord<String, ?> consumerRecord =  KafkaTestUtils.getSingleRecord(consumer, customerDeadTopic);
    Assert.isTrue(consumerRecord.key().equals("customer1"), "Customer key not matched");
  }

  private Map<String, String> getHeaders(ConsumerRecord<String, ?> consAfterRetry) {
    return StreamSupport.stream(consAfterRetry.headers().spliterator(), false)
      .collect(Collectors.toMap(Header::key, h -> new String(h.value(), StandardCharsets.UTF_8)));
  }

  private void processRecord(Integer delayInMinutes) {
    Assert.notNull(retryService, "Service is not injected");
    Customer customer = Customer.builder()
      .id(1)
      .name("customer1")
      .build();

    ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(customerTopic, customer.getName(), customer);
    kafkaTemplate.send(producerRecord);

    ConsumerRecord<String, ?> consumerRecord =  KafkaTestUtils.getSingleRecord(consumer,customerTopic);
    retryService.processAfter(delayInMinutes, customerTopic, consumerRecord);
  }

  @Data
  @Builder
  @AllArgsConstructor
  @NoArgsConstructor
  private static class Customer {
    private Integer id;
    private String name;
  }
}
