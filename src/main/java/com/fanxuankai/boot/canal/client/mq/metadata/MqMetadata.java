package com.fanxuankai.boot.canal.client.mq.metadata;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.fanxuankai.boot.canal.client.core.metadata.FilterMetadata;
import com.fanxuankai.boot.canal.client.mq.annotation.Mq;
import lombok.Getter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * MQ 注解元数据
 *
 * @author fanxuankai
 */
@Getter
public class MqMetadata {
    private String name = "";
    private List<CanalEntry.EventType> eventTypes = Collections.emptyList();
    private FilterMetadata filterMetadata = new FilterMetadata();

    public MqMetadata(Mq mq) {
        this.name = mq.name();
        this.eventTypes = Arrays.asList(mq.eventTypes());
        this.filterMetadata = new FilterMetadata(mq.filter());
    }

    public MqMetadata() {
    }
}
