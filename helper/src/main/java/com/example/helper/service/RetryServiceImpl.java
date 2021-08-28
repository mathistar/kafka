package com.example.helper.service;

import com.example.helper.config.KafkaHelperConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


@Slf4j
@Component
@RequiredArgsConstructor
public class RetryServiceImpl implements RetryService {
  private final KafkaListenerEndpointRegistry endpointRegistry;
  private final KafkaTemplate<String, Object> kafkaTemplate;
  private final KafkaHelperConfig helperConfig;
  // Scheduler to restart the kafka listener
  ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

  private void createHeaders(String topic, Integer retryCount, LocalDateTime creationTime, LocalDateTime executionTime, ProducerRecord<String, Object> producerRecord) {
    producerRecord.headers().add(new RecordHeader(EXECUTION_TIME, executionTime.toString().getBytes()));
    producerRecord.headers().add(new RecordHeader(CREATED_TIME, creationTime.toString().getBytes()));
    producerRecord.headers().add(new RecordHeader(TARGET_TOPIC, topic.getBytes()));
    producerRecord.headers().add(new RecordHeader(RETRY_COUNT, retryCount.toString().getBytes()));
  }

  private void sendMessage(String topic,
                           ConsumerRecord<String, Object> record,
                           LocalDateTime createdTime,
                           LocalDateTime executionTime,
                           Integer retryCount) {
    log.info("sending to topic: {}, key: {}, retry count: {}", topic, record.key(), retryCount);
    ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic, record.key(), record.value());
    createHeaders(topic, retryCount, createdTime, executionTime, producerRecord);
    kafkaTemplate.send(producerRecord).addCallback(
      (s) -> log.info("successfully message send to topic {}, key {}", topic, record.key()),
      ex -> log.error("Error Sending the Message into the original topic {}", topic, ex)
    );
  }


  /**
   * Kafka retry topic Listener to consume message and send the same message to orginal queue
   *
   * @param acknowledgment to commit the offset
   * @paran record
   * Message object
   */
  @KafkaListener(id = RETRY_CONTAINER_ID, topics = {"${kafka-helper.retry-topic}"}, groupId = "${kafka-helper.retry-group}", autoStartup = "${kafka-helper.enabled:true}")
  public void onMessage(ConsumerRecord<String, Object> record,
                        Acknowledgment acknowledgment,
                        @Header(CREATED_TIME) String createdTimeValue,
                        @Header(EXECUTION_TIME) String executionTimeValue,
                        @Header(RETRY_COUNT) String retryCountValue,
                        @Header(TARGET_TOPIC) String topic) {
    if (processMessage(record, createdTimeValue, executionTimeValue, retryCountValue, topic)) {
      acknowledgment.acknowledge();
    }
  }

  @Override
  public boolean processMessage(ConsumerRecord<String, Object> record, String createdTimeValue, String executionTimeValue, String retryCountValue, String topic) {
    LocalDateTime now = LocalDateTime.now();
    LocalDateTime createdTime = LocalDateTime.parse(createdTimeValue);
    LocalDateTime executionTime = LocalDateTime.parse(executionTimeValue).minusSeconds(1);
    Integer retryCount = Integer.parseInt(retryCountValue);
    log.info("inside the retry topic");
    if (!now.isAfter(executionTime)) {
      return false;
    }
    log.info("In retry topic key: {}, retry count: {}, now: {}:{}, execution: {}:{}",
      record.key(), retryCount, now.getMinute(), now.getSecond(), executionTime.getMinute(), executionTime.getSecond());
    if (retryCount > helperConfig.getMaxRetryCount()) {
      if (StringUtils.hasText(helperConfig.getDeadTopic())) {
        sendMessage(helperConfig.getDeadTopic(), record, createdTime, now, retryCount);
      } else {
        log.error("Maximum retry count {} reached and dead topic is not configured", helperConfig.getMaxRetryCount());
      }
    } else {
      sendMessage(topic, record, createdTime, now, retryCount);
    }
    return true;

  }

  @Override
  public void processAfter(Integer delayInMinutes, String topic, ConsumerRecord<String, ?> consumerRecord) {
    LocalDateTime now = LocalDateTime.now();
    LocalDateTime executionTime = now.plus(delayInMinutes, TimeUnit.MINUTES.toChronoUnit());
    Map<String, String> headers = KafkaHelperUtils.getHeaders(consumerRecord);
    int retryCount = Integer.parseInt(headers.computeIfAbsent(RETRY_COUNT, (k) -> "0"));
    LocalDateTime createDateTime = retryCount == 0 ? now : LocalDateTime.parse(headers.get(CREATED_TIME));
    ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(helperConfig.getRetryTopic(), consumerRecord.key(), consumerRecord.value());
    createHeaders(topic, retryCount + 1, createDateTime, executionTime, producerRecord);
    kafkaTemplate.send(producerRecord).addCallback(
      (s) -> scheduledExecutorService.schedule(this::startRetryListener, delayInMinutes, TimeUnit.MINUTES),
      ex -> log.error("Error Sending the Message into the retry queue {}", helperConfig.getRetryTopic(), ex)
    );

  }


  /**
   * scheduler to restart the Retry Listener
   */
  @Scheduled(cron = "${kafka-helper.retry-cron-job:0 0 0 * * *}")
  public void startRetryListener() {
    MessageListenerContainer listenerContainer = endpointRegistry.getListenerContainer(RETRY_CONTAINER_ID);
    if (listenerContainer != null && listenerContainer.isRunning()) {
      LocalDateTime now = LocalDateTime.now();
      log.info("Retry Listener will be restarted {}:{}", now.getMinute(), now.getSecond());
      listenerContainer.stop(listenerContainer::start);
    }
  }


}
  
