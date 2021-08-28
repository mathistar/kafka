package com.example.lib.kafka.demo.service;

import com.example.lib.kafka.demo.config.KafkaTopicConfig;
import com.example.lib.kafka.demo.domain.Customer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaProducerService {
  private final KafkaTemplate<String, Customer> kafkaTemplate;

  public ListenableFuture<SendResult<String, Customer>> sendCustomer(Customer customer) {
    String key = customer.getId().toString();
    ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(KafkaTopicConfig.CUSTOMER_TOPIC, key, customer);
    ListenableFuture<SendResult<String, Customer>> listenableFuture = kafkaTemplate.send(producerRecord);
    listenableFuture.addCallback(new ListenableFutureCallback<>() {
      @Override
      public void onFailure(Throwable ex) {
        handleFailure(key, customer, ex);
      }

      @Override
      public void onSuccess(SendResult<String, Customer> result) {
        handleSuccess(key, customer, result);
      }
    });
    return listenableFuture;
  }

  private void handleFailure(String key, Customer value, Throwable ex) {
    log.error("Error Sending the Message and the exception is {}", ex.getMessage());
    try {
      throw ex;
    } catch (Throwable throwable) {
      log.error("Error in OnFailure: {}", throwable.getMessage());
    }
  }

  private void handleSuccess(String key, Customer value, SendResult<String, Customer> result) {

  }
}
