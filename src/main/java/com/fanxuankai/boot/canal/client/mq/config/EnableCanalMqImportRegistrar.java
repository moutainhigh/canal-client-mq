package com.fanxuankai.boot.canal.client.mq.config;

import com.fanxuankai.boot.canal.client.mq.metadata.EnableCanalMqAttributes;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

/**
 * @author fanxuankai
 */
public class EnableCanalMqImportRegistrar implements ImportBeanDefinitionRegistrar {

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        EnableCanalMqAttributes.from(importingClassMetadata);
        MqConsumerScanner.scan();
        BeanRegistry.registerWith(registry);
    }
}
