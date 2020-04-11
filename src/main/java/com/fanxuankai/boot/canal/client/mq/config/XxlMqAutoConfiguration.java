package com.fanxuankai.boot.canal.client.mq.config;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.boot.canal.client.core.config.CanalRunner;
import com.fanxuankai.boot.canal.client.core.flow.FlowOtter;
import com.fanxuankai.boot.canal.client.core.flow.HandleSubscriber;
import com.fanxuankai.boot.canal.client.core.model.ConnectConfig;
import com.fanxuankai.boot.canal.client.core.protocol.MessageConsumer;
import com.fanxuankai.boot.canal.client.core.protocol.MessageHandler;
import com.fanxuankai.boot.canal.client.mq.consumer.xxl.XxlMqDeleteConsumer;
import com.fanxuankai.boot.canal.client.mq.consumer.xxl.XxlMqInsertConsumer;
import com.fanxuankai.boot.canal.client.mq.consumer.xxl.XxlMqUpdateConsumer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

/**
 * @author fanxuankai
 */
@Configuration
@Import({XxlMqInsertConsumer.class, XxlMqUpdateConsumer.class, XxlMqDeleteConsumer.class})
@EnableConfigurationProperties(CanalMqConfig.class)
@ConditionalOnProperty(prefix = "canal.mq", value = "type", havingValue = XxlMqAutoConfiguration.TYPE_NAME)
public class XxlMqAutoConfiguration {

    public static final String TYPE_NAME = "XxlMQ";

    @Resource
    private XxlMqInsertConsumer xxlMqInsertConsumer;
    @Resource
    private XxlMqUpdateConsumer xxlMqUpdateConsumer;
    @Resource
    private XxlMqDeleteConsumer xxlMqDeleteConsumer;

    public static class XxlMqFlowOtter extends FlowOtter {

        public XxlMqFlowOtter(ConnectConfig connectConfig, HandleSubscriber.Config handleSubscriberConfig) {
            super(connectConfig, handleSubscriberConfig);
        }
    }

    public static class XxlMqMessageHandler extends MessageHandler {
        public XxlMqMessageHandler(Config config) {
            super(config);
        }
    }

    @Bean
    public XxlMqFlowOtter rabbitMqFlowOtter(CanalMqConfig canalMqConfig, XxlMqMessageHandler mqMessageHandler) {
        HandleSubscriber.Config config = new HandleSubscriber.Config(mqMessageHandler,
                TYPE_NAME, canalMqConfig.getSkipMq());
        ConnectConfig connectConfig = new ConnectConfig(canalMqConfig.getInstance(),
                MqConsumerScanner.getFilter(), TYPE_NAME);
        return new XxlMqFlowOtter(connectConfig, config);
    }

    @Bean
    @SuppressWarnings("rawtypes")
    public XxlMqMessageHandler xxlMqMessageHandler() {
        Map<CanalEntry.EventType, MessageConsumer> consumerMap = new HashMap<>(3);
        consumerMap.put(CanalEntry.EventType.INSERT, xxlMqInsertConsumer);
        consumerMap.put(CanalEntry.EventType.UPDATE, xxlMqUpdateConsumer);
        consumerMap.put(CanalEntry.EventType.DELETE, xxlMqDeleteConsumer);
        MessageHandler.Config config = new MessageHandler.Config(consumerMap, TYPE_NAME);
        return new XxlMqMessageHandler(config);
    }

    @Bean
    public CanalRunner canalRunner(XxlMqFlowOtter xxlMqFlowOtter) {
        CanalRunner.Config config = new CanalRunner.Config(xxlMqFlowOtter, TYPE_NAME);
        return new CanalRunner(config);
    }
}
