package com.example.helper;

import lombok.RequiredArgsConstructor;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.persistence.EntityManagerFactory;

@Configuration
@ComponentScan("com.example.helper")
@EnableScheduling
@ConfigurationPropertiesScan
@ConditionalOnClass(EntityManagerFactory.class)
@RequiredArgsConstructor
public class KafkaHelperApplication {

  private final EntityManagerFactory entityManagerFactory;

  @Bean
  @ConditionalOnMissingBean
  public HibernateEventInvoker hibernateEventInvoker() {
    HibernateEventInvoker invoker = new HibernateEventInvoker();
    SessionFactoryImplementor sessionFactory = entityManagerFactory.unwrap(SessionFactoryImplementor.class);
    EventListenerRegistry registry = sessionFactory.getServiceRegistry().getService(EventListenerRegistry.class);
    registry.prependListeners(EventType.POST_UPDATE, invoker);
    registry.prependListeners(EventType.POST_INSERT, invoker);
    registry.prependListeners(EventType.POST_DELETE, invoker);
    return invoker;
  }
}
