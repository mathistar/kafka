package com.example.lib.kafka.demo.rest;

import com.example.lib.kafka.demo.domain.Customer;
import com.example.lib.kafka.demo.repository.CustomerRepository;
import com.example.lib.kafka.demo.service.KafkaProducerService;
import com.github.javafaker.Faker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.ZoneId;
import java.util.List;
import java.util.stream.IntStream;

@RestController
@Slf4j
@RequiredArgsConstructor
public class CustomerController {
  private final KafkaProducerService producerService;
  private final CustomerRepository customerRepository;

  @PostMapping("/customers")
  public ResponseEntity<Customer> postCustomer(@RequestBody Customer customer) {
    producerService.sendCustomer(customer);
    return ResponseEntity.status(HttpStatus.CREATED).body(customer);
  }

  @GetMapping("/api/trigger/customers/{count}")
  public ResponseEntity<String> postCustomer(@PathVariable Integer count) {
    Faker faker = new Faker();
    IntStream.range(0, count).forEach((i) -> {
      Customer customer = Customer.builder()
        .id(faker.random().nextInt(100))
        .dateOfBirth(faker.date().birthday().toInstant().atZone(ZoneId.systemDefault()).toLocalDate())
        .name(faker.name().name())
        .build();
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      producerService.sendCustomer(customer);
    });
    return ResponseEntity.status(HttpStatus.CREATED).body("Customer Created");

  }

  @PostMapping("/api/customers")
  public ResponseEntity<Customer> saveCustomer(@RequestBody Customer customer) {
    Customer savedCustomer = customerRepository.save(customer);
    return ResponseEntity.status(HttpStatus.CREATED).body(savedCustomer);
  }

  @GetMapping("/api/customers")
  public ResponseEntity<List<Customer>> saveCustomer() {
    List<Customer> allCustomer = customerRepository.findAll();
    return ResponseEntity.status(HttpStatus.OK).body(allCustomer);
  }

}
