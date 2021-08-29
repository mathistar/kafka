package com.example.lib.kafka.demo.service;

import com.example.lib.kafka.demo.domain.Address;
import com.example.lib.kafka.demo.domain.Customer;
import com.example.lib.kafka.demo.repository.AddressRepository;
import com.example.lib.kafka.demo.repository.CustomerRepository;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.util.List;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class CustomerService {
  private final CustomerRepository customerRepository;
  private final AddressRepository addressRepository;


  @Transactional
  @SneakyThrows
  public Customer saveCustomer(Customer customer) {
    Customer savedCustomer = customerRepository.save(customer);
    log.info("saved Customer id {}", customer.getId());
    if (customer.isError()) {
      throw new Exception("Error thrown on application");
    }
    return savedCustomer;
  }

  public List<Customer> getCustomer() {
    return customerRepository.findAll();
  }

  public Optional<Customer> getCustomerById(Integer id) {
    return customerRepository.findById(id);
  }

  @Transactional
  @SneakyThrows
  public Customer createCustomer(Customer customer, List<Address> addressList) {
//    customer.setAddressList(addressList);
    Customer savedCustomer = customerRepository.save(customer);
    addressRepository.saveAll(addressList);
//    savedCustomer.setAddressList(addressList);
//    log.info("customer object before saving - {}", savedCustomer);
    if (customer.isError()) {
      throw new Exception("Error thrown on application");
    }
    return savedCustomer;
  }

}
