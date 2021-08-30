package com.example.lib.kafka.demo.domain;

import com.example.helper.annotation.EnableKafkaMessage;
import com.example.lib.kafka.demo.config.KafkaTopicConfig;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;

import javax.persistence.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
@ToString(exclude = "customer")
@EnableKafkaMessage(topic = KafkaTopicConfig.ADDRESS_TOPIC)
public class Address {
  @Id
  @GeneratedValue(
    strategy = GenerationType.SEQUENCE,
    generator = "seq_post"
  )
  @SequenceGenerator(
    name = "seq_post",
    sequenceName = "ADDRESS_SEQUENCE",
    allocationSize = 5
  )
  private Integer id;
  private  String address;
  private String city;
  private Integer pinCode;
  @ManyToOne
  @JsonIgnore
  private Customer customer;
}
