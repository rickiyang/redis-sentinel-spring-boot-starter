package com.rickiyang.redis.config;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;


/**
 * @date: 2021/11/16 11:39 上午
 * @author: rickiyang
 * @Description:
 */
@Data
@ConfigurationProperties(prefix = RedisSentinelClientProperties.SENTINEL_PREFIX)
public class RedisSentinelClientProperties {
    public final static String SENTINEL_PREFIX = "rickiyang.redis.sentinel";
    private String masterName;
    private String sentinels;
    private long maxWait;
    private int maxIdle;
    private int maxActive;
    private boolean blockWhenExhausted;
    private long maxWaitMillis;
    private int maxTotal;
    private int minIdle;
    private long minEvictableIdleTimeMillis;
    private boolean testOnBorrow;
    private boolean testOnReturn;
    private boolean testWhileIdle;
    private int numTestsPerEvictionRun;
    private long softMinEvictableIdleTimeMillis;
    private long timeBetweenEvictionRunsMillis;
    private byte whenExhaustedAction;
}
