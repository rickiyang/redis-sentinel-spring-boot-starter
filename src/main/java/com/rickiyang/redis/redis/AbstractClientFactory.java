package com.rickiyang.redis.redis;
import redis.clients.jedis.JedisPool;

/**
 * @date: 2021/11/16 10:01 上午
 * @author: rickiyang
 * @Description:
 */
public abstract class AbstractClientFactory extends JedisPoolConfigAdapter {
    /**
     * 初始化
     */
    public abstract void init();

    /**
     * 获取主库连接池
     * @return
     */
    public abstract JedisPool getMasterPool();

    /**
     * 获取从库连接池
     * @return
     */
    public abstract JedisPool getSlavePool();

}