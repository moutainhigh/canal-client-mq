package com.fanxuankai.boot.canal.client.mq.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * canal参数配置
 *
 * @author fanxuankai
 */
@ConfigurationProperties(prefix = "canal.mq")
@Getter
@Setter
public class CanalMqConfig {

    /**
     * MQ 对应的 canal 实例名
     */
    private String instance = "example";

    /**
     * MQ 跳过处理
     */
    private Boolean skipMq = Boolean.FALSE;

    private String type = "RabbitMQ";

}
