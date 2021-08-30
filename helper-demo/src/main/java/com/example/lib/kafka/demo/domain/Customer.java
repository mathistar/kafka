package com.example.lib.kafka.demo.domain;

import com.example.helper.annotation.EnableKafkaMessage;
import com.example.lib.kafka.demo.config.KafkaTopicConfig;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.LocalDate;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
@EnableKafkaMessage(topic = KafkaTopicConfig.CUSTOMER_TOPIC)
public class Customer {
  @Id
  @GeneratedValue(
    strategy = GenerationType.SEQUENCE,
    generator = "seq_customer"
  )
  @SequenceGenerator(
    name = "seq_customer",
    sequenceName = "CUSTOMER_SEQUENCE",
    initialValue = 10000,
    allocationSize = 5
  )
  private Integer id;
  private  String name;
  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
  private LocalDate dateOfBirth;
  private String lastName;
  @Transient
  private String fullName;
  @Transient
  private boolean error;
  @OneToMany(mappedBy = "customer")
  private List<Address> addressList;
}
