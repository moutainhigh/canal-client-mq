package com.fanxuankai.boot.canal.client.mq.consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.boot.canal.client.core.metadata.CanalTableMetadata;
import com.fanxuankai.boot.canal.client.core.metadata.FilterMetadata;
import com.fanxuankai.boot.canal.client.core.protocol.MessageConsumer;
import com.fanxuankai.boot.canal.client.core.util.CommonUtils;
import com.fanxuankai.boot.canal.client.core.util.QueueNameUtils;
import com.fanxuankai.boot.canal.client.core.wrapper.EntryWrapper;
import com.fanxuankai.boot.canal.client.mq.config.MqConsumerScanner;
import com.fanxuankai.boot.canal.client.mq.metadata.MqMetadata;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Mq 抽象消费者
 *
 * @author fanxuankai
 */
public abstract class AbstractMqConsumer implements MessageConsumer<MessageInfo> {

    @Override
    public boolean canProcess(EntryWrapper entryWrapper) {
        return MqConsumerScanner.getMqConsumerClass(entryWrapper) != null;
    }

    @Override
    public FilterMetadata filter(EntryWrapper entryWrapper) {
        return MqConsumerScanner.getMetadata(entryWrapper).getFilterMetadata();
    }

    @Override
    public Class<?> getDomainType(EntryWrapper entryWrapper) {
        return MqConsumerScanner.getDomainType(entryWrapper);
    }

    protected String routingKey(EntryWrapper entryWrapper, CanalEntry.EventType eventType) {
        MqMetadata metadata = MqConsumerScanner.getMetadata(entryWrapper);
        if (StringUtils.isNotBlank(metadata.getName())) {
            return QueueNameUtils.customName(metadata.getName(), eventType);
        }
        Class<?> domainType = MqConsumerScanner.getDomainType(entryWrapper);
        CanalTableMetadata tableMetadata = MqConsumerScanner.getCanalTableMetadata(domainType);
        return QueueNameUtils.name(tableMetadata.getSchema(), tableMetadata.getName(), eventType);
    }

    protected String json(List<CanalEntry.Column> columnList) {
        return JSON.toJSONString(CommonUtils.toMap(columnList));
    }

    protected String json(List<CanalEntry.Column> beforeColumns, List<CanalEntry.Column> afterColumns) {
        Map<String, String> map0 = CommonUtils.toMap(beforeColumns);
        Map<String, String> map1 = CommonUtils.toMap(afterColumns);
        List<Object> list = new ArrayList<>(2);
        list.add(map0);
        list.add(map1);
        return new JSONArray(list).toJSONString();
    }

}
