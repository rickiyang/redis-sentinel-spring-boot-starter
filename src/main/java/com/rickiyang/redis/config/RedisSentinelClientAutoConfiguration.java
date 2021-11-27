package com.rickiyang.redis.config;

import com.google.common.collect.Sets;
import com.rickiyang.redis.annotation.EnableRedisSentinel;
import com.rickiyang.redis.redis.RedisClient;
import com.rickiyang.redis.redis.sentinel.RedisSentinelFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static com.rickiyang.redis.config.RedisSentinelClientProperties.SENTINEL_PREFIX;


/**
 * @date: 2021/11/16 9:52 上午
 * @author: rickiyang
 * @Description:
 */
@Slf4j
@Configuration
@ConditionalOnClass(EnableRedisSentinel.class)
@ConditionalOnProperty(prefix = SENTINEL_PREFIX, name = "masterName")
@EnableConfigurationProperties(RedisSentinelClientProperties.class)
public class RedisSentinelClientAutoConfiguration {

    @Resource
    RedisSentinelClientProperties redisSentinelClientProperties;

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public RedisSentinelFactory redisSentinelClientFactory() throws Exception {
        RedisSentinelFactory redisSentinelClientFactory = new RedisSentinelFactory();

        String[] sentinels = redisSentinelClientProperties.getSentinels().split(",");
        redisSentinelClientFactory.setMasterName(redisSentinelClientProperties.getMasterName());
        redisSentinelClientFactory.setServers(Sets.newHashSet(sentinels));
        reflectProperties(redisSentinelClientFactory);
        log.info("[init redis sentinel factory, redisSentinelClientProperties={}]", redisSentinelClientProperties);
        return redisSentinelClientFactory;
    }

    @Bean
    public RedisClient redisClient(RedisSentinelFactory redisSentinelFactory) throws Exception {
        return new RedisClient(redisSentinelFactory);
    }

    private String createGetMethodName(Field propertiesField, String fieldName) {
        String convertFieldName = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        return propertiesField.getType() == boolean.class ? "is" + convertFieldName : "get" + convertFieldName;
    }

    private String createSetMethodName(String fieldName) {
        String convertFieldName = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        return "set" + convertFieldName;
    }

    private boolean isPropertyBlank(Object value) {
        return value == null || "0".equals(value.toString()) || "false".equals(value.toString());
    }

    private void reflectProperties(RedisSentinelFactory redisSentinelClientFactory) throws Exception {
        Field[] propertiesFields = RedisSentinelClientProperties.class.getDeclaredFields();
        for (Field propertiesField : propertiesFields) {
            String fieldName = propertiesField.getName();
            if ("masterName".equals(fieldName) || "sentinels".equals(fieldName) || "SENTINEL_PREFIX".equals(fieldName)) {
                continue;
            }
            Method getMethod = RedisSentinelClientProperties.class.getMethod(createGetMethodName(propertiesField, fieldName));
            Object value = getMethod.invoke(redisSentinelClientProperties);
            if (!isPropertyBlank(value)) {
                Method setMethod = RedisSentinelFactory.class.getMethod(createSetMethodName(fieldName), propertiesField.getType());
                setMethod.invoke(redisSentinelClientFactory, value);
            }
        }
    }
}

