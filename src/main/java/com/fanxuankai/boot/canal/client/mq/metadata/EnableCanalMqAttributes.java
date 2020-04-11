package com.fanxuankai.boot.canal.client.mq.metadata;

import com.fanxuankai.boot.canal.client.mq.annotation.EnableCanalMq;
import com.fanxuankai.boot.canal.client.mq.enums.MqType;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;

import java.util.Map;

/**
 * EnableCanal 注解属性工具类
 *
 * @author fanxuankai
 */
public class EnableCanalMqAttributes {

    private static AnnotationAttributes attributes = new AnnotationAttributes();

    public static void from(AnnotationMetadata metadata) {
        Map<String, Object> annotationAttributes =
                metadata.getAnnotationAttributes(EnableCanalMq.class.getName(), false);
        attributes = AnnotationAttributes.fromMap(annotationAttributes);
        if (attributes == null) {
            throw new IllegalArgumentException(String.format(
                    "@%s is not present on importing class '%s' as expected",
                    EnableCanalMq.class.getSimpleName(), metadata.getClassName()));
        }
    }

    public static String getSchema() {
        return attributes.getString("schema");
    }

    public static String[] getBasePackages() {
        return attributes.getStringArray("basePackages");
    }

    public static MqType getMqType() {
        return attributes.getEnum("type");
    }

}
