package com.example.helper.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class KafkaHelperUtils {
  public static Map<String, String> getHeaders(ConsumerRecord<String, ?> record) {
    return StreamSupport.stream(record.headers().spliterator(), true).collect(
      Collectors.toMap(Header::key, h -> new String(h.value())));
  }
}
