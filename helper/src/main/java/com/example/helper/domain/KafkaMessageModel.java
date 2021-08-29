package com.example.helper.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaMessageModel<T> {
  private Operation operation;
  private String name;
  private T before;
  private T after;
}
