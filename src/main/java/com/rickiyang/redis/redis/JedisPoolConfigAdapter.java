package com.rickiyang.redis.redis;


import com.rickiyang.redis.util.RedisUtils;
import com.rickiyang.redis.util.ReflectUtils;
import redis.clients.jedis.JedisPoolConfig;

import java.lang.reflect.Field;
import java.time.Duration;

/**
 * @date: 2021/11/16 10:01 上午
 * @author: rickiyang
 * @Description:
 */
public abstract class JedisPoolConfigAdapter {
    protected JedisPoolConfig config = new JedisPoolConfig();

    public int getMaxIdle() {
        return config.getMaxIdle();
    }

    public void setMaxIdle(int maxIdle) {
        config.setMaxIdle(maxIdle);
    }

    public int getMinIdle() {
        return config.getMinIdle();
    }

    public void setMinIdle(int minIdle) {
        config.setMinIdle(minIdle);
    }

    public void setTestOnBorrow(boolean testOnBorrow) {
        config.setTestOnBorrow(testOnBorrow);
    }

    public void setTestOnReturn(boolean testOnReturn) {
        config.setTestOnReturn(testOnReturn);
    }

    public void setTestWhileIdle(boolean testWhileIdle) {
        config.setTestWhileIdle(testWhileIdle);
    }

    public long getTimeBetweenEvictionRunsMillis() {
        return config.getTimeBetweenEvictionRunsMillis();
    }

    public void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis) {
        config.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
    }

    public int getNumTestsPerEvictionRun() {
        return config.getNumTestsPerEvictionRun();
    }

    public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
        config.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
    }

    public long getMinEvictableIdleTimeMillis() {
        return config.getMinEvictableIdleTimeMillis();
    }

    public void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis) {
        config.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
    }

    public long getSoftMinEvictableIdleTimeMillis() {
        return config.getSoftMinEvictableIdleTimeMillis();
    }

    public void setSoftMinEvictableIdleTimeMillis(long softMinEvictableIdleTimeMillis) {
        config.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
    }

    private static Field maxTotalField = null;
    private static Field maxWaitField = null;
    private static Field testOnBorrowField = null;
    private static Field testOnReturnField = null;
    private static Field testWhileIdleField = null;
    private static Field whenExhaustedActionField = null;
    private static Field blockWhenExhaustedField = null;

    static {
        try {
            Class<?> jpc = JedisPoolConfig.class;
            if (1 == RedisUtils.versionOfCommonsPool()) {
                maxTotalField = ReflectUtils.getClassField(jpc, "maxActive", true);
                maxWaitField = ReflectUtils.getClassField(jpc, "maxWait", true);
                whenExhaustedActionField = ReflectUtils.getClassField(jpc, "whenExhaustedAction", true);
                whenExhaustedActionField.setAccessible(true);
            } else {
                maxTotalField = ReflectUtils.getClassField(jpc, "maxTotal", true);
                maxWaitField = ReflectUtils.getClassField(jpc, "maxWaitDuration", true);
                blockWhenExhaustedField = ReflectUtils.getClassField(jpc, "blockWhenExhausted", true);
                blockWhenExhaustedField.setAccessible(true);
            }
            testOnBorrowField = ReflectUtils.getClassField(jpc, "testOnBorrow", true);
            testOnReturnField = ReflectUtils.getClassField(jpc, "testOnReturn", true);
            testWhileIdleField = ReflectUtils.getClassField(jpc, "testWhileIdle", true);

            testOnBorrowField.setAccessible(true);
            testWhileIdleField.setAccessible(true);
            testOnReturnField.setAccessible(true);

            maxTotalField.setAccessible(true);
            maxWaitField.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setWhenExhaustedAction(byte whenExhaustedAction) {
        try {
            if (null != whenExhaustedActionField) {
                whenExhaustedActionField.setByte(config, whenExhaustedAction);
            } else if (null != blockWhenExhaustedField) {
                blockWhenExhaustedField.setBoolean(config, 1 == whenExhaustedAction);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setBlockWhenExhausted(boolean blockWhenExhausted) {
        try {
            if (null != whenExhaustedActionField) {
                whenExhaustedActionField.setByte(config, (byte) (blockWhenExhausted ? 1 : 0));
            } else if (null != blockWhenExhaustedField) {
                blockWhenExhaustedField.setBoolean(config, blockWhenExhausted);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public byte getWhenExhaustedAction() {
        try {
            if (null != whenExhaustedActionField) {
                return whenExhaustedActionField.getByte(config);
            } else if (null != blockWhenExhaustedField) {
                return (byte) (blockWhenExhaustedField.getBoolean(config) == true ? 1 : 0);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return 1;
    }

    public boolean getBlockWhenExhausted() {
        try {
            if (null != whenExhaustedActionField) {
                return whenExhaustedActionField.getByte(config) == 1 ? true : false;
            } else if (null != blockWhenExhaustedField) {
                return blockWhenExhaustedField.getBoolean(config);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    public boolean isTestWhileIdle() {
        try {
            return testWhileIdleField.getBoolean(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean getTestWhileIdle() {
        try {
            return testWhileIdleField.getBoolean(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isTestOnBorrow() {
        try {
            return testOnBorrowField.getBoolean(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean getTestOnBorrow() {
        try {
            return testOnBorrowField.getBoolean(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isTestOnReturn() {
        try {
            return testOnReturnField.getBoolean(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean getTestOnReturn() {
        try {
            return testOnReturnField.getBoolean(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int getMaxTotal() {
        try {
            return maxTotalField.getInt(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setMaxTotal(int maxTotal) {
        try {
            maxTotalField.setInt(config, maxTotal);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public long getMaxWaitMillis() {
        try {
            return maxWaitField.getLong(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setMaxWaitMillis(long maxWaitMillis) {
        try {
            maxWaitField.set(config, Duration.ofMillis(maxWaitMillis));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int getMaxActive() {
        try {
            return maxTotalField.getInt(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setMaxActive(int maxActive) {
        try {
            maxTotalField.setInt(config, maxActive);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public long getMaxWait() {
        try {
            return maxWaitField.getLong(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setMaxWait(long maxWait) {
        try {
            maxWaitField.setLong(config, maxWait);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
