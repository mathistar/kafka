package com.example.helper;

import com.example.helper.annotation.EnableKafkaMessage;
import com.example.helper.domain.KafkaMessageModel;
import com.example.helper.domain.Operation;
import com.example.helper.service.KafkaService;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.event.spi.*;
import org.hibernate.persister.entity.EntityPersister;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Service which registers and include entities for Kafka messaging
 */
@Slf4j
@RequiredArgsConstructor
public class HibernateEventInvoker implements
  PreInsertEventListener,
  PreUpdateEventListener,
  PreDeleteEventListener,
  PostInsertEventListener,
  PostDeleteEventListener,
  PostUpdateEventListener {
  private final KafkaService kafkaService;
  private final Map<Class<?>, EnableKafkaMessage> entityTopics = new HashMap<>();
  private final String basePackage = "com.example";

  @Override
  public boolean onPreDelete(PreDeleteEvent event) {
    return false;
  }

  @Override
  public boolean onPreInsert(PreInsertEvent event) {
    log.info("Pre insert - {}", event.getEntity());
    return false;
  }

  @Override
  public boolean onPreUpdate(PreUpdateEvent event) {
    log.info("Pre update - {}", event.getEntity());
    return true;
  }

  @Override
  public boolean requiresPostCommitHanding(EntityPersister entityPersister) {
    return false;
  }

  @Override
  public void onPostInsert(PostInsertEvent event) {
    onEvent(Operation.ADD, event.getEntity(), event.getPersister(), null);
  }

  @Override
  public void onPostDelete(PostDeleteEvent event) {
    onEvent(Operation.REMOVE, event.getEntity(), event.getPersister(), null);
  }

  @Override
  @SneakyThrows
  public void onPostUpdate(PostUpdateEvent event) {
    onEvent(Operation.MODIFY, event.getEntity(), event.getPersister(), event.getOldState());
  }

  private void onEvent(Operation operation, Object entity, EntityPersister persister, Object[] oldState) {
    Class<?> aClass = entity.getClass();
    if (!entityTopics.containsKey(aClass)) {
      return;
    }

    String name = entityTopics.get(aClass).name();
    String topic = entityTopics.get(aClass).topic();

    Object before = null;
    Object after = null;
    final String idName = persister.getIdentifierPropertyName();
    final Object fieldValue = getFieldValue(entity, idName);

    switch (operation) {
      case MODIFY:
        before = getObject(persister.getPropertyNames(), oldState, aClass);
        setFieldValue(before, idName, fieldValue);
        after = entity;
        break;
      case ADD:
        after = entity;
        break;
      case REMOVE:
        before = entity;
        break;
    }

    KafkaMessageModel<Object> message = KafkaMessageModel.builder()
      .operation(operation)
      .name(StringUtils.hasLength(name) ? name : aClass.getSimpleName())
      .before(before)
      .after(after)
      .build();
    log.info(" Kafka message object - {}", message);
    Assert.state(fieldValue != null, String.format("Id Field %s has null value in the entity %s", idName, name));
    kafkaService.sendMessage(topic, fieldValue.toString(), message);
  }

  @SneakyThrows
  private Object getObject(String[] propertyNames, Object[] values, Class<?> cls) {
    Object newInstance = cls.getDeclaredConstructor().newInstance();
    int i = 0;
    for (String fieldName : propertyNames) {
      setFieldValue(newInstance, fieldName, values[i]);
      i++;
    }
    return newInstance;

  }

  @SneakyThrows
  private boolean setFieldValue(Object object, String fieldName, Object fieldValue) {
    Class<?> clazz = object.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(object, fieldValue);
        return true;
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      }
    }
    return false;
  }

  @SneakyThrows
  private Object getFieldValue(Object object, String fieldName) {
    Class<?> clazz = object.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(object);
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      }
    }
    return null;
  }

  @PostConstruct
  @SneakyThrows
  public void init() {

    ClassPathScanningCandidateComponentProvider scanner =
      new ClassPathScanningCandidateComponentProvider(false);

    scanner.addIncludeFilter(new AnnotationTypeFilter(EnableKafkaMessage.class));

    for (BeanDefinition bd : scanner.findCandidateComponents(basePackage)) {
      Class<?> entityClass = Class.forName(bd.getBeanClassName());
      entityTopics.put(entityClass, entityClass.getAnnotation(EnableKafkaMessage.class));
    }
    log.info("Entity Topics - {}", entityTopics);
  }


}
