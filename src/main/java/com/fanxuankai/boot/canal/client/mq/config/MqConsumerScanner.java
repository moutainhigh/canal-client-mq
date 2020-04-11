package com.fanxuankai.boot.canal.client.mq.config;

import com.fanxuankai.boot.canal.client.core.annotation.CanalTable;
import com.fanxuankai.boot.canal.client.core.metadata.CanalTableMetadata;
import com.fanxuankai.boot.canal.client.core.util.CommonUtils;
import com.fanxuankai.boot.canal.client.core.util.ReflectionUtils;
import com.fanxuankai.boot.canal.client.core.wrapper.EntryWrapper;
import com.fanxuankai.boot.canal.client.mq.annotation.Mq;
import com.fanxuankai.boot.canal.client.mq.consumer.MqConsumer;
import com.fanxuankai.boot.canal.client.mq.metadata.EnableCanalMqAttributes;
import com.fanxuankai.boot.canal.client.mq.metadata.MqMetadata;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ConfigurationBuilder;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
@Slf4j
@SuppressWarnings("rawtypes")
public class MqConsumerScanner {

    public static void scan() {
        Reflections r =
                new Reflections(new ConfigurationBuilder()
                        .forPackages(EnableCanalMqAttributes.getBasePackages())
                        .setScanners(new SubTypesScanner())
                );
        long l = System.currentTimeMillis();
        Set<Class<? extends MqConsumer>> mqConsumers = r.getSubTypesOf(MqConsumer.class);
        log.info("Finished MqConsumer scanning in {}ms. Found {} MqConsumers.", System.currentTimeMillis() - l,
                mqConsumers.size());
        List<Bean> beans = Lists.newArrayList();
        for (Class<? extends MqConsumer> mqConsumerClass : mqConsumers) {
            Type[] genericInterfaces = mqConsumerClass.getGenericInterfaces();
            Class<?> domainType = null;
            for (Type genericInterface : genericInterfaces) {
                ParameterizedType p = (ParameterizedType) genericInterface;
                if (!Objects.equals(p.getRawType(), MqConsumer.class)) {
                    continue;
                }
                domainType = (Class<?>) p.getActualTypeArguments()[0];
            }
            if (domainType == null) {
                continue;
            }
            Mq mq = mqConsumerClass.getAnnotation(Mq.class);
            MqMetadata mqMetadata;
            if (mq == null) {
                mqMetadata = new MqMetadata();
            } else {
                mqMetadata = new MqMetadata(mq);
            }
            CanalTableMetadata canalTableMetadata;
            CanalTable canalTable = domainType.getAnnotation(CanalTable.class);
            String defaultSchema = EnableCanalMqAttributes.getSchema();
            if (canalTable == null) {
                canalTableMetadata = new CanalTableMetadata(defaultSchema, ReflectionUtils.getTableName(domainType));
            } else {
                canalTableMetadata = new CanalTableMetadata(canalTable, defaultSchema, domainType);
            }
            beans.add(new Bean(mqConsumerClass, domainType, mqMetadata, canalTableMetadata));
        }
        setup(beans);
    }

    public static final List<Class<? extends MqConsumer>> ALL_CLASSES = Lists.newArrayList();
    private static final Map<Class<? extends MqConsumer>, Class<?>> DOMAIN_TYPE_CACHE = Maps.newHashMap();
    private static final Map<String, Class<?>> DOMAIN_TYPE_BY_FULL_TABLE_NAME = Maps.newHashMap();
    private static final List<CanalTableMetadata> ALL_CANAL_TABLE_METADATA = Lists.newArrayList();
    private static final Map<Class<?>, CanalTableMetadata> CANAL_TABLE_METADATA_CACHE = Maps.newHashMap();
    private static final Map<String, MqMetadata> METADATA_CACHE = Maps.newHashMap();
    private static final Map<String, Class<? extends MqConsumer>> Class_BY_FULL_TABLE_NAME = Maps.newHashMap();

    public static Class<?> getDomainType(Class<? extends MqConsumer> mqConsumerClass) {
        return DOMAIN_TYPE_CACHE.get(mqConsumerClass);
    }

    public static Class<?> getDomainType(EntryWrapper entryWrapper) {
        return DOMAIN_TYPE_BY_FULL_TABLE_NAME.get(CommonUtils.fullTableName(entryWrapper.getSchemaName(),
                entryWrapper.getTableName()));
    }

    public static String getFilter() {
        return ALL_CANAL_TABLE_METADATA.stream()
                .map(canalTableMetadata ->
                        CommonUtils.fullTableName(canalTableMetadata.getSchema(), canalTableMetadata.getName()))
                .collect(Collectors.joining(","));
    }

    public static Class<? extends MqConsumer> getMqConsumerClass(EntryWrapper entryWrapper) {
        return Class_BY_FULL_TABLE_NAME.get(CommonUtils.fullTableName(entryWrapper.getSchemaName(),
                entryWrapper.getTableName()));
    }

    public static MqMetadata getMetadata(EntryWrapper entryWrapper) {
        return METADATA_CACHE.get(CommonUtils.fullTableName(entryWrapper.getSchemaName(),
                entryWrapper.getTableName()));
    }

    public static CanalTableMetadata getCanalTableMetadata(Class<?> domainType) {
        return CANAL_TABLE_METADATA_CACHE.get(domainType);
    }

    private static void setup(List<Bean> beans) {
        if (CollectionUtils.isEmpty(beans)) {
            return;
        }
        for (Bean bean : beans) {
            Class<? extends MqConsumer> mqConsumerClass = bean.mqConsumerClass;
            Class<?> domainType = bean.domainType;
            CanalTableMetadata canalTableMeta = bean.canalTableMeta;
            DOMAIN_TYPE_CACHE.put(mqConsumerClass, domainType);
            ALL_CANAL_TABLE_METADATA.add(canalTableMeta);
            String fullTableName = CommonUtils.fullTableName(canalTableMeta.getSchema(), canalTableMeta.getName());
            Class_BY_FULL_TABLE_NAME.put(fullTableName, mqConsumerClass);
            METADATA_CACHE.put(fullTableName, bean.mqMetadata);
            CANAL_TABLE_METADATA_CACHE.put(domainType, canalTableMeta);
            DOMAIN_TYPE_BY_FULL_TABLE_NAME.put(fullTableName, domainType);
            ALL_CLASSES.add(mqConsumerClass);
        }
    }

    private static record Bean(Class<? extends MqConsumer>mqConsumerClass, Class<?>domainType,
                               MqMetadata mqMetadata, CanalTableMetadata canalTableMeta) {

    }

}
