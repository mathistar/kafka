package com.example.lib.kafka.demo.service;

import com.example.helper.service.RetryService;
import com.example.lib.kafka.demo.config.KafkaTopicConfig;
import com.example.lib.kafka.demo.domain.Customer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.lang.Nullable;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Optional;

@Component
@Slf4j
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "kafka-helper", value = "enabled", havingValue = "true", matchIfMissing = true)
public class ConsumerComponent {
  private final RetryService retryService;

  @KafkaListener(topics = {KafkaTopicConfig.CUSTOMER_TOPIC}, groupId = "customer_group")
  public void onMessage(ConsumerRecord<String, Customer> consumerRecord,
                        Acknowledgment acknowledgment,
                        @Nullable @Header(RetryService.RETRY_COUNT) String retryCountValue) {
    int retryCount = Integer.parseInt(Optional.ofNullable(retryCountValue).orElse("0"));
    log.info("Customer in main : {} and retry count : {}", consumerRecord.key(), retryCount);
    boolean nextTry = RandomUtils.nextBoolean();
    if (nextTry) {
      log.info("Retry process started ");
      retryService.processAfter(20, KafkaTopicConfig.CUSTOMER_TOPIC, consumerRecord);
    } else {
      log.info ("No retry - message processed");
    }
    acknowledgment.acknowledge();
  }

  @KafkaListener(topics = {KafkaTopicConfig.CUSTOMER_DEAD_TOPIC}, groupId = "dead-topic-group")
  public void onMessage(ConsumerRecord<String, Object> record,
                        Acknowledgment acknowledgment,
                        @Header(RetryService.CREATED_TIME) String createdTimeValue,
                        @Header(RetryService.EXECUTION_TIME) String executionTimeValue,
                        @Header(RetryService.RETRY_COUNT) String retryCountValue,
                        @Header(RetryService.TARGET_TOPIC) String topic
  ) {
    LocalDateTime createdTime = LocalDateTime.parse(createdTimeValue);
    LocalDateTime executionTime = LocalDateTime.parse(executionTimeValue);
    Integer retryCount = Integer.parseInt(retryCountValue);
    log.info("Dead Log retry count {} and customer {}", retryCount, record.value());
    log.info("Dead Log create time {}:{} and execution Time {}:{} and target topic {}",
      createdTime.getMinute(), createdTime.getSecond(), executionTime.getMinute(), executionTime.getSecond(), topic);
    acknowledgment.acknowledge();
  }
}
