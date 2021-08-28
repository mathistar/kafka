package com.example.helper.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kafka-helper")
@Getter
@Setter
public class KafkaHelperConfig {
  private String retryTopic;
  private String retryGroup;
  private String retryCronJob;
  private String deadTopic;
  private Integer maxRetryCount = 5;
  private boolean enabled=true;
}
