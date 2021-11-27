package com.rickiyang.redis.redis;


import com.rickiyang.redis.exception.CsRedisRuntimeException;
import com.rickiyang.redis.util.RedisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @date: 2021/11/16 10:05 上午
 * @author: rickiyang
 * @Description: 管理redis的连接池工厂类，redis连接的参数设置
 */
public class RedisClientFactory extends AbstractClientFactory {

    private static final Logger log = LoggerFactory.getLogger(RedisClientFactory.class);

    private volatile List<JedisPool> redisMasterPool = new ArrayList<>();

    private volatile List<JedisPool> redisSlavePool = new ArrayList<>();

    private int totalServersSize;

    private int masterServerSize;

    private int slaveServerSize;

    private volatile int realServersCount = 0;

    private ReentrantLock lock = new ReentrantLock();

    private AtomicInteger atomicMasterCount = new AtomicInteger(0);
    private AtomicInteger atomicSlaveCount = new AtomicInteger(0);

    private List<String> redisServers;

    private boolean healthCheck;
    private long checkPeriod = RedisClientFactoryHealthChecker.CHECK_PERIOD;
    private long fullCheckPeriod = RedisClientFactoryHealthChecker.CHECK_PERIOD * 4;
    private static final ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(2);
    private volatile ScheduledFuture<?> healthCheckFuture = null;

    private String alarmReportId;
    private String alarmProgressName;

    /**
     * 构造器函数
     *
     * @param redisServers
     *            redis服务器地址列表 ip:port:username:password
     */
    public RedisClientFactory(List<String> redisServers) {
        super();
        if (redisServers == null || redisServers.size() == 0) {
            throw new CsRedisRuntimeException("redisServers couldn't be null");
        }
        this.redisServers = redisServers;
        this.totalServersSize = redisServers.size();
        init();
    }

    public RedisClientFactory() {

    }

    /**
     * 从redisMasterPool中随机获取pool
     *
     * @return
     *         Master的jedisPool资源池
     */
    @Override
    public JedisPool getMasterPool() {

        if (masterServerSize <= 0) {
            return getSlavePool(true);
        }
        int currentIndex = atomicMasterCount.getAndIncrement();
        if (currentIndex < 0) {
            currentIndex = 0 - currentIndex;
        }
        if (masterServerSize > 0) {
            currentIndex = currentIndex % masterServerSize;
        }
        JedisPool jedisPool = redisMasterPool.get(currentIndex);
        return jedisPool;
    }

    /**
     * 从redisSlavePool中随机获取pool,当前pool无法获取jedis连接时，切换到其他的Jedispool
     *
     * @return
     *         Slave的jedisPool资源池
     */
    @Override
    public JedisPool getSlavePool() {
        return getSlavePool(false);
    }

    private JedisPool getSlavePool(boolean fromMaster) {
        if (slaveServerSize <= 0) {
            if (fromMaster) {
                throw new CsRedisRuntimeException("no avalible jedisPool");
            } else {
                return getMasterPool();
            }
        }

        int currentIndex = atomicSlaveCount.getAndIncrement();
        if (currentIndex < 0) {
            currentIndex = 0 - currentIndex;
        }
        currentIndex = currentIndex % slaveServerSize;
        JedisPool jedisPool = redisSlavePool.get(currentIndex);
        return jedisPool;
    }

    public void setRedisServers(List<String> redisServers) {
        if (redisServers == null || redisServers.size() == 0) {
            throw new CsRedisRuntimeException("redisServers couldn't be null");
        }
        this.redisServers = redisServers;
        this.totalServersSize = redisServers.size();
    }

    /**
     * 初始化
     */
    @Override
    public void init() {

        if (this.totalServersSize == 0) {
            throw new IllegalArgumentException("redisServer is invalidly config,please correctly set Redis Servers.");
        }

        /**
         * 在Master池上操作时,如果异常会重新init.操作master有可能会被并发。
         */
        if (lock.isLocked()) {
            return;
        }
        lock.lock();
        try {
            JedisPool pool = null;
            Jedis jedis = null;
            List<JedisPool> newMasterPool = new ArrayList<>();
            List<JedisPool> newRslavePool = new ArrayList<>();
            StringBuilder sb = new StringBuilder();

            Map<String, Integer> initialPools = new HashMap<>();
            for (int i = 0; i < totalServersSize; i++) {
                String[] strArray = RedisUtils.parseServerInfo(redisServers.get(i));
                String[] ips = RedisUtils.parseServerIp(strArray[0]);
                int port = Integer.parseInt(strArray[1]);
                String password = strArray[2];
                // 默认是10秒
                int timeout = strArray[3] != null && !"".equals(strArray[3].trim()) ? Integer.valueOf(strArray[3])
                        : 10000;
                sb.append(strArray[0]).append(":").append(port).append(",");
                password = "".equals(password) ? null : password;

                if (ips.length < 1) {
                    log.warn("config wrong " + redisServers.get(i));
                    continue;
                }
                String ip = null;
                String key = ips[0] + ":" + port;

                // 减少多个ip:port重复配置引起的重复初始化生成poolObject
                if (initialPools.containsKey(key)) {
                    log.warn("skip duplicate config for " + redisServers.get(i));
                    continue;
                }
                initialPools.put(key, 1);
                for (int j = 0; j < ips.length; j++) {
                    ip = ips[j];
                    try {
                        jedis = new Jedis(ip, port, timeout);
                        jedis.connect();
                        if (null != password) {
                            jedis.auth(password);
                        }
                        boolean isMaster = false;
                        try {
                            String info = jedis.info();
                            isMaster = RedisUtils.isMaster(info);
                        } catch (Throwable e) {
                            log.warn("can not support info function.", e);
                        }
                        pool = RedisUtils.getJedisPool(this.config, ip, port, timeout, password);
                        // 主实例
                        if (isMaster == true) {
                            newMasterPool.add(pool);
                            // 从实例
                        } else {
                            newRslavePool.add(pool);
                        }
                        break;
                    } catch (Exception e) {
                        log.warn("[" + ip + ":" + port + "]" + e.getMessage(), e);
                        continue;
                    } finally {
                        try {
                            if (null != jedis && jedis.isConnected()) {
                                jedis.disconnect();
                            }
                        } catch (Exception e) {

                        }
                        jedis = null;
                    }
                }
            }
            realServersCount = initialPools.size();
            List<JedisPool> oldMasterPool = redisMasterPool;
            List<JedisPool> oldRslavePool = redisSlavePool;
            redisMasterPool = newMasterPool;
            redisSlavePool = newRslavePool;
            // 如果没有slave 避免用户直接获取slave进行操作导致错误
            if (redisSlavePool.size() == 0) {
                redisSlavePool = redisMasterPool;
            }
            if (null != oldMasterPool && oldMasterPool.size() > 0) {
                destroy(oldMasterPool);
            }
            if (null != oldRslavePool && oldRslavePool.size() > 0) {
                destroy(oldRslavePool);
            }
            this.masterServerSize = redisMasterPool.size();
            this.slaveServerSize = redisSlavePool.size();
            startHealthCheck();
        } finally {
            lock.unlock();
        }
    }


    private void startHealthCheck() {
        if (!healthCheck) {
            stopHealthCheck();
            return;
        }
        if (null == healthCheckFuture || healthCheckFuture.isCancelled()) {
            if (checkPeriod <= 0) {
                checkPeriod = RedisClientFactoryHealthChecker.CHECK_PERIOD;
            }
            if (fullCheckPeriod <= 0) {
                fullCheckPeriod = 4 * checkPeriod;
            }
            healthCheckFuture = timer.scheduleWithFixedDelay(
                    new RedisClientFactoryHealthChecker(this, checkPeriod, fullCheckPeriod), checkPeriod, checkPeriod,
                    TimeUnit.MILLISECONDS);
        }
    }

    private void stopHealthCheck() {
        if (null != healthCheckFuture) {
            if (!healthCheckFuture.isCancelled()) {
                healthCheckFuture.cancel(true);
                timer.purge();
                healthCheckFuture = null;
            }
        }
    }

    /**
     * 销毁Jedis资源池
     */
    public void destroy(List<JedisPool> pool) {
        if (pool != null && pool.size() != 0) {
            for (JedisPool p : pool) {
                try {
                    p.destroy();
                } catch (Throwable e) {
                    log.warn(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * 销毁操作
     */
    public void destroy() {
        if (redisMasterPool != null && redisMasterPool.size() != 0) {
            for (JedisPool p : redisMasterPool) {
                p.destroy();
            }
        }
        if (redisSlavePool != null && redisSlavePool.size() != 0) {
            for (JedisPool p : redisSlavePool) {
                p.destroy();
            }
        }
        stopHealthCheck();
    }

    public int getRealServersCount() {
        return realServersCount;
    }

    public List<String> getRedisServers() {
        return redisServers;
    }

    public List<JedisPool> getRedisMasterPool() {
        return redisMasterPool;
    }

    public List<JedisPool> getRedisSlavePool() {
        return redisSlavePool;
    }

    public int getTotalServersSize() {
        return totalServersSize;
    }

    public int getMasterServerSize() {
        return masterServerSize;
    }

    public int getSlaveServerSize() {
        return slaveServerSize;
    }

    public boolean isHealthCheck() {
        return healthCheck;
    }

    public void setHealthCheck(boolean healthCheck) {
        this.healthCheck = healthCheck;
    }

    public String getAlarmReportId() {
        return alarmReportId;
    }

    public String getAlarmProgressName() {
        return alarmProgressName;
    }

    public void setAlarmReportId(String alarmReportId) {
        this.alarmReportId = alarmReportId;
    }

    public void setAlarmProgressName(String alarmProgressName) {
        this.alarmProgressName = alarmProgressName;
    }

    public long getCheckPeriod() {
        return checkPeriod;
    }

    public long getFullCheckPeriod() {
        return fullCheckPeriod;
    }

    public void setCheckPeriod(long checkPeriod) {
        this.checkPeriod = checkPeriod;
    }

    public void setFullCheckPeriod(long fullCheckPeriod) {
        this.fullCheckPeriod = fullCheckPeriod;
    }

}

