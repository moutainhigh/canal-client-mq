package com.fanxuankai.boot.canal.client.mq.consumer.xxl;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.boot.canal.client.core.wrapper.EntryWrapper;
import com.fanxuankai.boot.canal.client.mq.consumer.MessageInfo;

import java.util.List;
import java.util.stream.Collectors;

/**
 * XXL-MQ 新增事件消费者
 *
 * @author fanxuankai
 */
public class XxlMqInsertConsumer extends AbstractXxlMqConsumer {

    @Override
    public MessageInfo process(EntryWrapper entryWrapper) {
        String topic = routingKey(entryWrapper, CanalEntry.EventType.INSERT);
        List<String> messages = entryWrapper.getAllRowDataList()
                .stream()
                .map(rowData -> json(rowData.getAfterColumnsList()))
                .collect(Collectors.toList());
        return new MessageInfo(topic, messages);
    }
}
