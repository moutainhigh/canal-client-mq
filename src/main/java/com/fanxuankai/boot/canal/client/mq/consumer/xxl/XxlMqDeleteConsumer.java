package com.fanxuankai.boot.canal.client.mq.consumer.xxl;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.boot.canal.client.core.wrapper.EntryWrapper;
import com.fanxuankai.boot.canal.client.mq.consumer.MessageInfo;

import java.util.List;
import java.util.stream.Collectors;

/**
 * XXL-MQ 删除事件消费者
 *
 * @author fanxuankai
 */
public class XxlMqDeleteConsumer extends AbstractXxlMqConsumer {

    @Override
    public MessageInfo process(EntryWrapper entryWrapper) {
        String topic = routingKey(entryWrapper, CanalEntry.EventType.DELETE);
        List<String> messages = entryWrapper.getAllRowDataList()
                .stream()
                .map(rowData -> json(rowData.getBeforeColumnsList()))
                .collect(Collectors.toList());
        return new MessageInfo(topic, messages);
    }
}
