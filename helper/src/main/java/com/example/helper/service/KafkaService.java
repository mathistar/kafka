package com.example.helper.service;

import com.example.helper.domain.KafkaMessageModel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaService {
  private final KafkaTemplate<String, KafkaMessageModel<Object>> kafkaTemplate;

  public ListenableFuture<SendResult<String, KafkaMessageModel<Object>>> sendMessage(String topic, String key, KafkaMessageModel<Object> KafkaMessageModel) {
    ProducerRecord<String, KafkaMessageModel<Object>> producerRecord = new ProducerRecord<>(topic, key, KafkaMessageModel);
    ListenableFuture<SendResult<String, KafkaMessageModel<Object>>> listenableFuture = kafkaTemplate.send(producerRecord);
    listenableFuture.addCallback(new ListenableFutureCallback<>() {
      @Override
      public void onFailure(Throwable ex) {
       log.error("failure on publish in kafka - ", ex);
      }

      @Override
      public void onSuccess(SendResult<String, KafkaMessageModel<Object>> result) {
        final RecordMetadata recordMetadata = result.getRecordMetadata();
        log.info("Message been send to {} - {}:{}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
      }
    });
    return listenableFuture;
  }
}
