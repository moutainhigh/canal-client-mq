package com.fanxuankai.boot.canal.client.mq.consumer;

import java.util.List;

/**
 * 消息实体
 *
 * @author fanxuankai
 */
public record MessageInfo(String routingKey, List<String>messages) {
}