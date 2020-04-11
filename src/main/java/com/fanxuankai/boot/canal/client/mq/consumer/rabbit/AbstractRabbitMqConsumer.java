package com.fanxuankai.boot.canal.client.mq.consumer.rabbit;

import com.fanxuankai.boot.canal.client.mq.consumer.AbstractMqConsumer;
import com.fanxuankai.boot.canal.client.mq.consumer.MessageInfo;
import org.springframework.amqp.core.AmqpTemplate;

import javax.annotation.Resource;

/**
 * RabbitMQ 抽象消费者
 *
 * @author fanxuankai
 */
public abstract class AbstractRabbitMqConsumer extends AbstractMqConsumer {

    @Resource
    protected AmqpTemplate amqpTemplate;

    @Override
    public void consume(MessageInfo messageInfo) {
        messageInfo.messages().forEach(s -> amqpTemplate.convertAndSend(messageInfo.routingKey(), s));
    }

}
