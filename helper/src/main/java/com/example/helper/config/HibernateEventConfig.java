package com.example.helper.config;

import com.example.helper.HibernateEventInvoker;
import com.example.helper.service.KafkaService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManagerFactory;

@ConditionalOnClass(EntityManagerFactory.class)
@RequiredArgsConstructor
@Configuration
@Slf4j
public class HibernateEventConfig {
  private final EntityManagerFactory entityManagerFactory;
  private final KafkaService kafkaService;

  @Bean
  @ConditionalOnMissingBean
  public HibernateEventInvoker hibernateEventInvoker() {
    log.info("Initializing the Hibernate Event Invoker");
    HibernateEventInvoker invoker = new HibernateEventInvoker(kafkaService);
    SessionFactoryImplementor sessionFactory = entityManagerFactory.unwrap(SessionFactoryImplementor.class);
    EventListenerRegistry registry = sessionFactory.getServiceRegistry().getService(EventListenerRegistry.class);
    registry.prependListeners(EventType.PRE_UPDATE, invoker);
    registry.prependListeners(EventType.PRE_INSERT, invoker);
    registry.prependListeners(EventType.PRE_DELETE, invoker);
    registry.prependListeners(EventType.POST_UPDATE, invoker);
    registry.prependListeners(EventType.POST_INSERT, invoker);
    registry.prependListeners(EventType.POST_DELETE, invoker);
    return invoker;
  }
}
