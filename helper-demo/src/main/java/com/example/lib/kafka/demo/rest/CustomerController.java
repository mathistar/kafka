package com.example.lib.kafka.demo.rest;

import com.example.lib.kafka.demo.domain.Address;
import com.example.lib.kafka.demo.domain.Customer;
import com.example.lib.kafka.demo.service.CustomerService;
import com.example.lib.kafka.demo.service.KafkaProducerService;
import com.github.javafaker.Faker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RestController
@Slf4j
@RequiredArgsConstructor
public class CustomerController {
  private final KafkaProducerService producerService;
  private final CustomerService customerService;

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
    Customer savedCustomer = customerService.saveCustomer(customer);
    return ResponseEntity.status(HttpStatus.CREATED).body(savedCustomer);
  }

  @GetMapping("/api/customers")
  public ResponseEntity<List<Customer>> getCustomer() {
    List<Customer> allCustomer = customerService.getCustomer();
    return ResponseEntity.status(HttpStatus.OK).body(allCustomer);
  }

  @GetMapping("/api/customers/get/{id}")
  public ResponseEntity<Customer> getCustomerById(@PathVariable Integer id) {
    Optional<Customer> customerById = customerService.getCustomerById(id);
    Customer customer = customerById.orElse(new Customer());

    return ResponseEntity.status(HttpStatus.OK).body(customer);
  }

  @GetMapping(value = {"/api/customers/create","/api/customers/create/{id}"})
  public ResponseEntity<Customer> createCustomer(@PathVariable Optional<Integer> id) {
    Customer savedCustomer = getSavedCustomer(id.orElse(null), null, false);
//    log.info("before sending the response - {}", savedCustomer);
    return ResponseEntity.status(HttpStatus.OK).body(savedCustomer);
  }

  private Customer getSavedCustomer(Integer id, String name, boolean error) {
    Faker faker = new Faker();
    Customer customer = Customer.builder()
      .dateOfBirth(faker.date().birthday().toInstant().atZone(ZoneId.systemDefault()).toLocalDate())
      .name(name != null ? null : faker.name().name())
      .error(error)
      .fullName(faker.name().fullName())
      .lastName(faker.name().lastName())
      .build();
    if (id != null) {
      customer.setId(id);
    }
    List<Address> addressList = IntStream.range(0, faker.random().nextInt(1, 4)).mapToObj((i) -> Address.builder()
      .address(faker.address().streetAddress())
      .city(faker.address().city())
      .pinCode(faker.random().nextInt(500001, 900000))
//      .customer(customer)
      .build()).collect(Collectors.toList());


    Customer savedCustomer = customerService.createCustomer(customer, addressList);
    return savedCustomer;
  }

  @GetMapping({"/api/customers/create_error","/api/customers/create_error/{id}", "/api/customers/create_error/{id}/{error}"})
  public ResponseEntity<Customer> createCustomerError(@PathVariable Optional<Integer> id, @PathVariable Optional<Boolean> error) {
    Customer savedCustomer = getSavedCustomer(id.orElse(null), "error", error.orElse(false));
//    log.info("before sending the response - {}", savedCustomer);
    return ResponseEntity.status(HttpStatus.OK).body(savedCustomer);
  }

}
