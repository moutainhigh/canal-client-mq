package com.fanxuankai.boot.canal.client.mq.config;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.boot.canal.client.core.config.CanalRunner;
import com.fanxuankai.boot.canal.client.core.flow.FlowOtter;
import com.fanxuankai.boot.canal.client.core.flow.HandleSubscriber;
import com.fanxuankai.boot.canal.client.core.model.ConnectConfig;
import com.fanxuankai.boot.canal.client.core.protocol.MessageConsumer;
import com.fanxuankai.boot.canal.client.core.protocol.MessageHandler;
import com.fanxuankai.boot.canal.client.mq.consumer.rabbit.RabbitMqDeleteConsumer;
import com.fanxuankai.boot.canal.client.mq.consumer.rabbit.RabbitMqInsertConsumer;
import com.fanxuankai.boot.canal.client.mq.consumer.rabbit.RabbitMqUpdateConsumer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

/**
 * RabbitMQ  Otter 并行流客户端
 *
 * @author fanxuankai
 */
@Configuration
@Import({RabbitMqInsertConsumer.class, RabbitMqUpdateConsumer.class, RabbitMqDeleteConsumer.class})
@EnableConfigurationProperties(CanalMqConfig.class)
@ConditionalOnProperty(prefix = "canal.mq", value = "type", havingValue = RabbitMqAutoConfiguration.TYPE_NAME)
public class RabbitMqAutoConfiguration {

    public static final String TYPE_NAME = "RabbitMQ";

    @Resource
    private RabbitMqInsertConsumer rabbitMqInsertConsumer;
    @Resource
    private RabbitMqUpdateConsumer rabbitMqUpdateConsumer;
    @Resource
    private RabbitMqDeleteConsumer rabbitMqDeleteConsumer;

    public static class RabbitMqFlowOtter extends FlowOtter {
        public RabbitMqFlowOtter(ConnectConfig connectConfig, HandleSubscriber.Config handleSubscriberConfig) {
            super(connectConfig, handleSubscriberConfig);
        }
    }

    public static class RabbitMqMessageHandler extends MessageHandler {
        public RabbitMqMessageHandler(Config config) {
            super(config);
        }
    }

    @Bean
    public RabbitMqFlowOtter rabbitMqFlowOtter(CanalMqConfig canalMqConfig, RabbitMqMessageHandler mqMessageHandler) {
        HandleSubscriber.Config config = new HandleSubscriber.Config(mqMessageHandler,
                TYPE_NAME, canalMqConfig.getSkipMq());
        ConnectConfig connectConfig = new ConnectConfig(canalMqConfig.getInstance(),
                MqConsumerScanner.getFilter(), TYPE_NAME);
        return new RabbitMqFlowOtter(connectConfig, config);
    }

    @Bean
    @SuppressWarnings("rawtypes")
    public RabbitMqMessageHandler rabbitMqMessageHandler() {
        Map<CanalEntry.EventType, MessageConsumer> consumerMap = new HashMap<>(3);
        consumerMap.put(CanalEntry.EventType.INSERT, rabbitMqInsertConsumer);
        consumerMap.put(CanalEntry.EventType.UPDATE, rabbitMqUpdateConsumer);
        consumerMap.put(CanalEntry.EventType.DELETE, rabbitMqDeleteConsumer);
        MessageHandler.Config config = new MessageHandler.Config(consumerMap, TYPE_NAME);
        return new RabbitMqMessageHandler(config);
    }

    @Bean
    public CanalRunner canalRunner(RabbitMqFlowOtter rabbitMqFlowOtter) {
        CanalRunner.Config config = new CanalRunner.Config(rabbitMqFlowOtter, TYPE_NAME);
        return new CanalRunner(config);
    }

}
