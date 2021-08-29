package com.example.helper;

import com.example.helper.annotation.EnableKafkaMessage;
import com.example.helper.domain.KafkaMessageModel;
import com.example.helper.domain.Operation;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.event.spi.*;
import org.hibernate.persister.entity.EntityPersister;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Service which registers and include entities for Kafka messaging
 */
@Slf4j
public class HibernateEventInvoker implements
        PostInsertEventListener,
        PostDeleteEventListener,
        PostUpdateEventListener {

    private final Map<Class<?>, EnableKafkaMessage> entityTopics = new HashMap<>();
    private final String basePackage = "com.example";


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

    private void onEvent( Operation operation, Object entity, EntityPersister persister, Object[] oldState) {
        Class<?> aClass = entity.getClass();
        if (!entityTopics.containsKey(aClass)) {
            return;
        }

        String name = entityTopics.get(aClass).name();

        Object before = null;
        Object after = null;
        switch (operation) {
            case MODIFY:
                before = getObject(persister.getPropertyNames(), oldState, aClass);
                final String idName = persister.getIdentifierPropertyName();
                setFieldValue(before, idName, getFieldValue(entity, idName));
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
        log.info (" Kafka message object - {}", message);
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

        for (BeanDefinition bd : scanner.findCandidateComponents(basePackage))
        {
            Class<?> entityClass = Class.forName(bd.getBeanClassName());
            entityTopics.put(entityClass, entityClass.getAnnotation(EnableKafkaMessage.class));
        }
        log.info("Entity Topics - {}", entityTopics);
    }

}
