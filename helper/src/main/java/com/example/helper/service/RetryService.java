package com.example.helper.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface RetryService {

  String RETRY_CONTAINER_ID = "retryContainer";
  String EXECUTION_TIME = "executionTime";
  String CREATED_TIME = "createdTime";
  String RETRY_COUNT = "retryCount";
  String TARGET_TOPIC = "targetTopic";

  boolean processMessage(ConsumerRecord<String, Object> record, String createdTimeValue, String executionTimeValue, String retryCountValue, String topic);

  void processAfter(Integer delayInMinutes, String topic, ConsumerRecord<String, ?> consumerRecord);
}
