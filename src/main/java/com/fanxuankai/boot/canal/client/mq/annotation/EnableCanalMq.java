package com.fanxuankai.boot.canal.client.mq.annotation;

import com.fanxuankai.boot.canal.client.mq.config.EnableCanalMqImportRegistrar;
import com.fanxuankai.boot.canal.client.mq.enums.MqType;
import org.springframework.context.annotation.Import;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author fanxuankai
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Import(EnableCanalMqImportRegistrar.class)
public @interface EnableCanalMq {

    /**
     * @return 数据库名
     */
    String schema();

    /**
     * @return 基础包
     */
    String[] basePackages();

    /**
     * @return 消息队列类型
     */
    MqType type() default MqType.RABBIT_MQ;
}
