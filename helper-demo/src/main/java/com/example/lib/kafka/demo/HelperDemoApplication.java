package com.example.lib.kafka.demo;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
@RequiredArgsConstructor
public class HelperDemoApplication {
  public static void main(String[] args) {
    SpringApplication.run(HelperDemoApplication.class, args);
  }
}
