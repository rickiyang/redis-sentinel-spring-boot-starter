package com.rickiyang.redis.redis.sentinel;

import com.rickiyang.redis.redis.AbstractClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @date: 2021/11/16 11:43 上午
 * @author: rickiyang
 * @Description: Redis Sentinel工厂类
 */

public class RedisSentinelFactory extends AbstractClientFactory {

    private static final Logger logger = LoggerFactory.getLogger(RedisSentinelFactory.class);

    private CustomJedisSentinelPool masterPool;
    private int retries = 3;

    private String masterName;
    private Set<String> servers;
    private String password = null;
    private int timeout = 8000;

    private ReentrantLock lock = new ReentrantLock();

    public RedisSentinelFactory() {
        // 初始值
        this.config.setMaxWaitMillis(6000L);
        this.config.setTestOnBorrow(true);
    }

    /**
     * 初始化
     */
    @Override
    public void init() {
        checkArguments();
        // RedisClient 异常情况下调用init方法存在并发的问题
        if (!lock.tryLock()) {
            return;
        }
        try {
            CustomJedisSentinelPool old = masterPool;
            masterPool = new CustomJedisSentinelPool(masterName, servers, this.config, timeout, password);
            if (old != null) {
                old.destroy(); // 不destroy会导致重复创建后台线程
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public JedisPool getMasterPool() {
        return masterPool;
    }

    @Override
    public JedisPool getSlavePool() {
        return getReaderPool();
    }

    private void checkArguments() {
        if (servers == null || servers.size() < 0) {
            throw new IllegalArgumentException(
                    "sentinel server configs " + "sentinelServers:{" + servers + "} should not be null");
        }

        if (masterName == null || "".equals(masterName.trim())) {
            throw new IllegalArgumentException("masterName config should not be null");
        }

        password = "".equals(password) ? null : password;
    }

    /**
     * 获取Master实例
     *
     * @return
     */
    public Jedis getMaster() {
        for (int i = 0; i <= retries; i++) {
            Jedis jedis = null;
            try {
                jedis = masterPool.getResource();
                return jedis;
            } catch (JedisConnectionException jce) {
                logger.error("[getMaster] sentinelPool.getResource() error", jce);
                if (jedis != null) {
                    masterPool.returnBrokenResource(jedis);
                    jedis = null;
                }
            }
        }
        return null;
    }

    /**
     * 从Jedis池中获取1个Pool
     *
     * @return
     */
    public JedisPool getReaderPool() {
        JedisPool pool = null;
        for (int i = 0; i < retries; i++) {
            pool = masterPool.getReaderPool();
            if (pool != null) {
                return pool;
            }
        }
        // return master pool when can not get the reader pool
        return masterPool;
    }

    /**
     * 获取只读Jedis实例
     *
     * @return
     */
    public Jedis getReader() {
        JedisPool readPool = null;
        Jedis jedis = null;
        try {
            // 从不同的实例拿
            for (int i = 0; i < retries; i++) {
                try {
                    readPool = getReaderPool();
                    if (readPool != null) {
                        jedis = readPool.getResource();
                    }
                    if (jedis != null) {
                        break;
                    }
                } catch (JedisConnectionException e) {
                    if (readPool != null && null != jedis) {
                        jedis.close();
                        jedis = null;
                        logger.warn("get jedis instance timeout from pool failure,try next slave,error:{}",
                                e.getCause());
                    }
                }
            }
            // 从 master 拿
            if (jedis == null) {
                jedis = masterPool.getResource();
            }
            if (jedis == null) {
                logger.error(" neither master pool nor slave pool can get jedis instance");
            } else {
                return jedis;
            }
        } catch (JedisConnectionException jce) {
            logger.error("[getReader] jedisPool.getResource() error:{}", jce.getCause());
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;

    }

    public void returnSentinelResource(Jedis jedis) {
        masterPool.returnResource(jedis);
    }

    public void returnBrokenResource(Jedis jedis) {
        masterPool.returnBrokenResource(jedis);
    }

    public void returnJedisResource(Jedis jedis) {
        if (jedis != null)
            jedis.close();
    }

    public void destroy() {
        if (masterPool != null) {
            masterPool.destroy();
        }
    }

    public String getMasterName() {
        return masterName;
    }

    public void setMasterName(String masterName) {
        this.masterName = masterName;
    }

    public Set<String> getServers() {
        return servers;
    }

    public void setServers(Set<String> servers) {
        this.servers = servers;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

}

