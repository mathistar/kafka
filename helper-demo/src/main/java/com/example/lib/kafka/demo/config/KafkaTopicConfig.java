package com.example.lib.kafka.demo.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile("local")
@RequiredArgsConstructor
public class KafkaTopicConfig {
  public static final String CUSTOMER_TOPIC = "customer-topic";
  public static final String CUSTOMER_RETRY_TOPIC = "customer-retry-topic";
  public static final String CUSTOMER_DEAD_TOPIC = "customer-dead-topic";

  @Bean
  public NewTopic customerRetryTopics() {
    return TopicBuilder.name(CUSTOMER_RETRY_TOPIC)
      .partitions(1)
      .replicas(1)
      .build();
  }

  @Bean
  public NewTopic customerTopics() {
    return TopicBuilder.name(CUSTOMER_TOPIC)
      .partitions(1)
      .replicas(1)
      .build();
  }

  @Bean
  public NewTopic customerDeadTopics() {
    return TopicBuilder.name(CUSTOMER_DEAD_TOPIC)
      .partitions(1)
      .replicas(1)
      .build();
  }
}
