package com.example.helper.annotation;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EnableKafkaMessage {

  String topic();

  String name() default "";
}
