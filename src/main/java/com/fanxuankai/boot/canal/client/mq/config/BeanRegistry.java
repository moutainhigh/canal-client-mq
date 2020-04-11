package com.fanxuankai.boot.canal.client.mq.config;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.boot.canal.client.core.metadata.CanalTableMetadata;
import com.fanxuankai.boot.canal.client.core.util.QueueNameUtils;
import com.fanxuankai.boot.canal.client.mq.consumer.MqConsumer;
import com.fanxuankai.boot.canal.client.mq.enums.MqType;
import com.fanxuankai.boot.canal.client.mq.metadata.EnableCanalMqAttributes;
import com.fanxuankai.boot.canal.client.mq.util.JavassistBeanGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;

/**
 * @author fanxuankai
 */
@SuppressWarnings("rawtypes")
@Slf4j
public class BeanRegistry {

    /**
     * 自动生成 MQ 消费者且注册为 Spring bean
     *
     * @param registry BeanDefinitionRegistry
     */
    public static void registerWith(BeanDefinitionRegistry registry) {
        MqType mqType = EnableCanalMqAttributes.getMqType();
        for (Class<? extends MqConsumer> mqConsumerClass : MqConsumerScanner.ALL_CLASSES) {
            Class<?> domainType = MqConsumerScanner.getDomainType(mqConsumerClass);
            CanalTableMetadata tableMetadata = MqConsumerScanner.getCanalTableMetadata(domainType);
            MqConsumer<?> mqConsumer;
            try {
                mqConsumer = mqConsumerClass.getConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException("MqConsumer 无空构造器");
            }
            String topic = QueueNameUtils.name(tableMetadata.getSchema(), tableMetadata.getName());
            if (mqType == MqType.RABBIT_MQ) {
                register(JavassistBeanGenerator.generateRabbitMqConsumer(domainType, topic), mqConsumer, registry);
            } else if (mqType == MqType.XXL_MQ) {
                register(JavassistBeanGenerator.generateXxlMqConsumer(domainType, topic, CanalEntry.EventType.INSERT)
                        , mqConsumer, registry);
                register(JavassistBeanGenerator.generateXxlMqConsumer(domainType, topic, CanalEntry.EventType.UPDATE)
                        , mqConsumer, registry);
                register(JavassistBeanGenerator.generateXxlMqConsumer(domainType, topic, CanalEntry.EventType.DELETE)
                        , mqConsumer, registry);
            }
        }
    }

    private static void register(Class<?> mqConsumerBeanClass, MqConsumer<?> mqConsumer,
                                 BeanDefinitionRegistry registry) {
        RootBeanDefinition beanDefinition = new RootBeanDefinition(mqConsumerBeanClass);
        ConstructorArgumentValues cav = beanDefinition.getConstructorArgumentValues();
        cav.addGenericArgumentValue(mqConsumer);
        BeanDefinitionHolder bh = new BeanDefinitionHolder(beanDefinition, mqConsumerBeanClass.getName());
        BeanDefinitionReaderUtils.registerBeanDefinition(bh, registry);
    }
}
