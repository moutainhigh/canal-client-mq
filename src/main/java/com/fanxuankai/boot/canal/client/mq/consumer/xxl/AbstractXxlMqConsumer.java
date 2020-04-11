package com.fanxuankai.boot.canal.client.mq.consumer.xxl;

import com.fanxuankai.boot.canal.client.mq.consumer.AbstractMqConsumer;
import com.fanxuankai.boot.canal.client.mq.consumer.MessageInfo;
import com.xxl.mq.client.message.XxlMqMessage;
import com.xxl.mq.client.producer.XxlMqProducer;

/**
 * XXL-MQ 抽象消费者
 *
 * @author fanxuankai
 */
public abstract class AbstractXxlMqConsumer extends AbstractMqConsumer {

    @Override
    public void consume(MessageInfo messageInfo) {
        messageInfo.messages().forEach(s -> XxlMqProducer.produce(new XxlMqMessage(messageInfo.routingKey(), s)));
    }

}
