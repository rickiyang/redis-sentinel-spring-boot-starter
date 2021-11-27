package com.rickiyang.redis.redis;

import com.rickiyang.redis.exception.CsRedisRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.util.*;

/**
 * @date: 2021/11/16 9:59 上午
 * @author: rickiyang
 * @Description:
 */
public class RedisClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisClient.class);
    private AbstractClientFactory factory;

    public AbstractClientFactory getFactory() {
        return factory;
    }

    public void setFactory(AbstractClientFactory factory) {
        this.factory = factory;
    }

    /**
     * redis客户端构造器
     *
     * @param factory redis工厂类 {@link RedisClientFactory}
     */
    public RedisClient(AbstractClientFactory factory) {
        super();
        this.factory = factory;
    }

    public RedisClient() {

    }

    private void exceptionHandler(JedisPool jedisPool, Jedis jedis, Exception e) {
        // 增加一层 try catch ，防止 io 异常，导致初始化失败
        jedisClose(jedis);
    }

    private void jedisClose(Jedis jedis) {
        try {
            if (jedis != null) {
                jedis.close();
            }
        } catch (Exception e1) {
            LOGGER.warn("jedis.close failed", e1);
        }
    }

    /**
     * 从redisMasterPool中随机获取pool
     *
     * @return Salve的jedis资源池
     */
    public JedisPool getJedisMasterPool() {
        if (factory == null) {
            throw new IllegalArgumentException("Initial a redisClient should first init a RedisClientFactory object,"
                    + " but the factory not be null!");
        }
        return factory.getMasterPool();
    }

    /**
     * 从redisSlavePool中随机获取pool
     *
     * @return Master的jedis资源池
     */
    public JedisPool getJedisSlavePool() {
        if (factory == null) {
            throw new IllegalArgumentException("Initial a redisClient should first init a RedisClientFactory object,"
                    + " but the factory not be null!");
        }
        return factory.getSlavePool();
    }

    /**
     * 执行set操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param dbIndex redis db index
     * @param key     set的key值
     * @param value   key对应的value值
     * @return 返回被设置的值 String类型
     */
    public String setAndReturn(int dbIndex, final String key, String value) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();

            // 如果为0,则不需通信表明select db0
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.set(key, value);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis set fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 执行set操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param key   set的key值
     * @param value key对应的value值
     * @return 设置成功则返回被设置值value
     */
    public String setAndReturn(final String key, String value) {
        return setAndReturn(0, key, value);
    }

    /**
     * 使用setnx命令探测
     *
     * @param dbIndex
     * @param key
     * @param value
     * @param expire  过期秒数,当key设置成功且expire大于0，才会设置key的过期时间
     * @return Long, 1-设置成功，0-key已存在
     */
    public long setnx(int dbIndex, String key, String value, int expire) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();

            // 如果为0,则不需通信表明select db0
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            long t = jedis.setnx(key, value);
            if (t == 1 && expire > 0) {
                jedis.expire(key, expire);
            }
            return t;
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis setnx fail", e);
        } finally {
            jedisClose(jedis);
        }
    }


    /**
     * 返回对应key的ttl（Time To Live），
     *
     * @param dbIndex
     * @param key
     * @return Long，-2：key不存在，-1：key不会过期，>0剩余过期秒数
     */
    public long ttl(int dbIndex, String key) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();

            // 如果为0,则不需通信表明select db0
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            long t = jedis.ttl(key);
            return t;
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis ttl fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 在redis上执行相应的lua脚本
     *
     * @param dbIndex
     * @param readonly 如果为true并且存在从库设置的情况下，在从库上执行
     * @param script   lua脚本
     * @param keys
     * @param args
     * @return Object 根据lua脚本实际返回类型决定
     */
    public Object eval(int dbIndex, boolean readonly, String script, List<String> keys, List<String> args) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            if (readonly && null != getJedisSlavePool()) {
                jedisPool = getJedisSlavePool();
            } else {
                jedisPool = getJedisMasterPool();
            }
            jedis = jedisPool.getResource();

            // 如果为0,则不需通信表明select db0
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            Object obj = jedis.eval(script, keys, args);
            return obj;
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis eval fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 执行set操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param key     set的key值
     * @param value   key对应的value值
     * @param dbIndex redis db index
     * @param seconds 有效时间
     * @return 设置成功则返回 OK
     */
    public String setAndReturn(int dbIndex, final String key, String value, int seconds) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();

            // 如果为0,则不需通信表明select db0
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.setex(key, seconds, value);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis set fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 执行set操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param key     set的key值
     * @param value   key对应的value值
     * @param seconds 有效时间
     * @return 设置成功则 "OK"
     */
    public String setAndReturn(final String key, String value, int seconds) {
        return setAndReturn(0, key, value, seconds);
    }

    /**
     * 执行set操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param key     set的key值
     * @param value   key对应的value值
     * @param dbIndex redis db index
     * @param seconds 有效时间
     * @return 设置成功则返回被设置值value
     */
    public String setAndReturn(int dbIndex, final byte[] key, byte[] value, int seconds) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();

            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.setex(key, seconds, value);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis set fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 执行set操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param key     set的key值
     * @param value   key对应的value值
     * @param seconds 有效时间
     * @return 设置成功则返回被设置值value
     */
    public String setAndReturn(final byte[] key, byte[] value, int seconds) {
        return setAndReturn(0, key, value, seconds);
    }

    /**
     * 获取info信息
     *
     * @return String类型，当前连接服务器的服务器信息
     */
    public String infoAndReturn() {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();

            return jedis.info();
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis info fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 在从库执行get操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param dbIndex db的索引值
     * @param key     set的key值
     * @return 返回key对应的value值
     */
    public String getAndReturn(int dbIndex, final String key) {
        return getAndReturnHandler(Boolean.FALSE, dbIndex, key);
    }

    /**
     * 在主库执行get操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param dbIndex db的索引值
     * @param key     set的key值
     * @return 返回key对应的value值
     */
    public String getAndReturnFromMaster(int dbIndex, final String key) {
        return getAndReturnHandler(Boolean.TRUE, dbIndex, key);
    }

    private String getAndReturnHandler(boolean fromMaster, int dbIndex, final String key) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            if (fromMaster) {
                jedisPool = getJedisMasterPool();
            } else {
                jedisPool = getJedisSlavePool();
            }
            jedis = jedisPool.getResource();

            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.get(key);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis get fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 在主库执行get操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param key set的key值
     * @return 返回key对应的value值
     */
    public String getAndReturnFromMaster(final String key) {
        return getAndReturnFromMaster(0, key);
    }

    /**
     * 在从库执行get操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param key set的key值
     * @return 返回key对应的value值
     */
    public String getAndReturn(final String key) {
        return getAndReturn(0, key);
    }

    /**
     * 执行set操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param dbIndex redis db index
     * @param key     set的key值
     * @param value   key对应的value值
     * @return String类型, 返回key对应的value值
     */
    public String setAndReturn(int dbIndex, final byte[] key, byte[] value) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();

            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.set(key, value);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis set fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 执行set操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param key   set的key值
     * @param value key对应的value值
     * @return 返回key对应的value值
     */
    public String setAndReturn(final byte[] key, byte[] value) {
        return setAndReturn(0, key, value);
    }

    /**
     * 执行get操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param dbIndex db的索引值
     * @param key     set的key值
     * @return 返回key对应的value值, 字节数组类型
     */
    public byte[] getAndReturn(int dbIndex, final byte[] key) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisSlavePool();
            jedis = jedisPool.getResource();

            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.get(key);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis get fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 执行get操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param key set的key值
     * @return 返回key对应的value值, 字节数组类型
     */
    public byte[] getAndReturn(final byte[] key) {
        return getAndReturn(0, key);
    }

    /**
     * 执行mset操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param dbIndex    db的索引值
     * @param keysValues set的key值
     * @return Status code reply Basically +OK as MSET can't fail
     */
    public String mSetAndReturn(int dbIndex, String... keysValues) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();

            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.mset(keysValues);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException(e.getMessage(), e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 执行mset操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param keysValues set的key值
     * @return Status code reply Basically +OK as MSET can't fail
     */
    public String mSetAndReturn(String... keysValues) {
        return mSetAndReturn(0, keysValues);
    }

    /**
     * 执行mget操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param dbIndex db的索引值
     * @param keys    set的keys,可以获取多个key的value值
     * @return 根据keys获取的values, 返回值为String型的List
     */
    public List<String> mGetAndReturn(int dbIndex, String... keys) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisSlavePool();
            jedis = jedisPool.getResource();

            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.mget(keys);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis mget fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 执行mset操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param keys set的keys,可以获取多个key的value值
     * @return 根据keys获取的values, 返回值为String型的List
     */
    public List<String> mGetAndReturn(String... keys) {
        return mGetAndReturn(0, keys);
    }

    /**
     * 执行mset操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param dbIndex    db的索引值
     * @param keysValues set的keys,可以获取多个key的value值
     * @return Status code reply Basically +OK as MSET can't fail
     */
    public String mSetAndReturn(int dbIndex, byte[]... keysValues) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();

            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.mset(keysValues);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis mset fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 执行mset操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param keysValues set的keys,可以获取多个key的value值
     * @return Status code reply Basically +OK as MSET can't fail
     */
    public String mSetAndReturn(byte[]... keysValues) {
        return mSetAndReturn(0, keysValues);
    }

    /**
     * 执行mget操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param dbIndex db的索引值
     * @param keys    set的keys,可以获取多个key的value值
     * @return 根据keys获取的values, 返回值为byte型的List
     */
    public List<byte[]> mGetAndReturn(int dbIndex, byte[]... keys) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();

            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.mget(keys);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis mget fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 执行mset操作，然后释放client连接
     * </br>
     * 如果要多次操作，请使用原生的Jedis, 可以使用 getJedisMasterPool getJedisSlavePool 获取pool后，再获取redis连接
     * </br>
     * 并在调用完成后，需调用pool的returnResource方法释放该连接
     *
     * @param keys set的keys,可以获取多个key的value值
     * @return 根据keys获取的values, 返回值为byte型的List
     */
    public List<byte[]> mGetAndReturn(byte[]... keys) {
        return mGetAndReturn(0, keys);
    }

    /**
     * 执行smembers操作，然后释放client连接
     * 返回集合 key 中的所有成员。不存在的 key 被视为空集合
     *
     * @param key set的key值
     * @return 默认db下, 当前key对应的所有成员
     */
    public Set<String> smembers(String key) {
        return smembers(0, key);
    }

    /**
     * 执行smembers操作，然后释放client连接
     * 返回集合 key 中的所有成员。不存在的 key 被视为空集合
     *
     * @param dbIndex
     * @param key     set的key值
     * @return 默认db下, 当前key对应的所有成员
     */
    public Set<String> smembers(int dbIndex, String key) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisSlavePool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.smembers(key);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis get fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * redis SADD 操作，向名为key的set中添加一个或者多个value
     *
     * @param key    假如 key不存在，则创建一个只包含 member 元素作成员的集合。当 key 不是集合类型时，返回一个错误。
     * @param values 要存入的value
     * @return 被添加到集合中的新元素的数量，不包括被忽略的元素。
     */
    public Long sadd(String key, String... values) {
        return sadd(0, key, values);
    }

    /**
     * redis SADD 操作，向名为key的set中添加一个或者多个value
     *
     * @param dbIndex
     * @param key     假如 key不存在，则创建一个只包含 member 元素作成员的集合。当 key 不是集合类型时，返回一个错误。
     * @param values  要存入的value
     * @return 被添加到集合中的新元素的数量，不包括被忽略的元素。
     */
    public Long sadd(int dbIndex, String key, String... values) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.sadd(key, values);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis set fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * redis的 SREM 操作，移除set(名称为key)中的一个或多个value
     *
     * @param key    set的key值
     * @param values 要被删除的value值
     * @return 被删除的元素的个数
     */
    public Long srem(String key, String... values) {
        return srem(0, key, values);
    }

    /**
     * redis的 SREM 操作，移除set(名称为key)中的一个或多个value
     *
     * @param dbIndex
     * @param key     set的key值
     * @param values  要被删除的value值
     * @return 被删除的元素的个数
     */
    public Long srem(int dbIndex, String key, String... values) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.srem(key, values);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis set fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * Redis 的SCARD 操作，返回当前set中的value个数。
     * 集合的基数。
     * 当 key 不存在时，返回 0
     *
     * @param key set的key值
     * @return Long
     * set中这个key对应的value的个数
     */
    public Long scard(String key) {
        return scard(0, key);
    }

    /**
     * Redis 的SCARD 操作，返回当前set中的value个数。
     * 集合的基数。
     * 当 key 不存在时，返回 0
     *
     * @param dbIndex
     * @param key     set的key值
     * @return Long
     * set中这个key对应的value的个数
     */
    public Long scard(int dbIndex, String key) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisSlavePool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.scard(key);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis get fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * SISMEMBER key member 判断 member 元素是否集合 key 的成员。
     * 如果 member 元素是集合的成员，返回 1 。
     * 如果 member 元素不是集合的成员，或 key 不存在，返回 0 。
     *
     * @param key   set的key值
     * @param value 被判断的字符串value
     * @return boolean值
     */
    public Boolean sismember(String key, String value) {
        return sismember(0, key, value);
    }

    /**
     * SISMEMBER key member 判断 member 元素是否集合 key 的成员。
     * 如果 member 元素是集合的成员，返回 1 。
     * 如果 member 元素不是集合的成员，或 key 不存在，返回 0 。
     *
     * @param dbIndex
     * @param key     set的key值
     * @param value   被判断的字符串value
     * @return boolean值
     */
    public Boolean sismember(int dbIndex, String key, String value) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisSlavePool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.sismember(key, value);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis get fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /******* Hash Operation **********/

    /**
     * 将哈希表 key 中的域 field 的值设为 value 。如果 key 不存在，一个新的哈希表被创建并进行 HSET 操作。
     * 如果域 field 已经存在于哈希表中，旧值将被覆盖。
     *
     * @param key   hash表的标记key
     * @param field hash表中的存储对象的key
     * @param value hash表中的存储对象的key对应的value
     * @return 如果 field 是哈希表中的一个新建域，并且值设置成功，返回 1 。
     * 如果哈希表中域 field 已经存在且旧值已被新值覆盖，返回 0 。
     */
    public Long hset(String key, String field, String value) {
        return hset(0, key, field, value);
    }

    /**
     * 将哈希表 key 中的域 field 的值设为 value 。如果 key 不存在，一个新的哈希表被创建并进行 HSET 操作。
     * 如果域 field 已经存在于哈希表中，旧值将被覆盖。
     *
     * @param dbIndex
     * @param key     hash表的标记key
     * @param field   hash表中的存储对象的key
     * @param value   hash表中的存储对象的key对应的value
     * @return 如果 field 是哈希表中的一个新建域，并且值设置成功，返回 1 。
     * 如果哈希表中域 field 已经存在且旧值已被新值覆盖，返回 0 。
     */
    public Long hset(int dbIndex, String key, String field, String value) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.hset(key, field, value);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis set fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 同时将多个 field-value (域-值)对设置到哈希表 key 中。此命令会覆盖哈希表中已存在的域。
     * 如果 key 不存在，一个空哈希表被创建并执行 HMSET 操作。
     *
     * @param key   hash表的标记key
     * @param value 多个 field-value (域-值)对
     * @return 如果命令执行成功，返回 OK 。
     * 当 key 不是哈希表(hash)类型时，返回一个错误。
     */
    public String hmset(String key, Map<String, String> value) {
        return hmset(0, key, value);
    }

    /**
     * 同时将多个 field-value (域-值)对设置到哈希表 key 中。此命令会覆盖哈希表中已存在的域。
     * 如果 key 不存在，一个空哈希表被创建并执行 HMSET 操作。
     *
     * @param dbIndex
     * @param key     hash表的标记key
     * @param value   多个 field-value (域-值)对
     * @return 如果命令执行成功，返回 OK 。
     * 当 key 不是哈希表(hash)类型时，返回一个错误。
     */
    public String hmset(int dbIndex, String key, Map<String, String> value) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.hmset(key, value);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis set fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 返回哈希表 key 中给定域 field 的值。
     *
     * @param key   hash表的标记key
     * @param field hash表中的某个存储空间的标识key
     * @return 给定域的值。当给定域不存在或是给定 key 不存在时，返回 nil 。
     */
    public String hget(String key, String field) {
        return hget(0, key, field);
    }

    /**
     * 返回哈希表 key 中给定域 field 的值。
     *
     * @param dbIndex
     * @param key     hash表的标记key
     * @param field   hash表中的某个存储空间的标识key
     * @return 给定域的值。当给定域不存在或是给定 key 不存在时，返回 nil 。
     */
    public String hget(int dbIndex, String key, String field) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisSlavePool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.hget(key, field);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis get fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    public Map<String, String> hgetAll(String key) {
        return hgetAll(0, key);
    }

    /**
     * 返回哈希表 key 中所有域 field 的值。
     *
     * @param dbIndex
     * @param key     hash表的标记key
     * @return 给定域的值。当给定域不存在或是给定 key 不存在时，返回 空 map 。
     */
    public Map<String, String> hgetAll(int dbIndex, String key) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisSlavePool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.hgetAll(key);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis get hgetAll " + key, e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 返回哈希表 key 中，一个或多个给定域的值。
     * 如果给定的域不存在于哈希表，那么返回一个 nil 值。
     * 因为不存在的 key 被当作一个空哈希表来处理，所以对一个不存在的 key 进行 HMGET 操作将返回一个只带有 nil 值的表。
     *
     * @param key    hash表的标记key
     * @param fields 多个给定域的标识fields
     * @return 一个包含多个给定域的关联值的表，表值的排列顺序和给定域参数的请求顺序一样
     */
    public List<String> hmget(String key, String... fields) {
        return hmget(0, key, fields);
    }

    /**
     * 返回哈希表 key 中，一个或多个给定域的值。
     * 如果给定的域不存在于哈希表，那么返回一个 nil 值。
     * 因为不存在的 key 被当作一个空哈希表来处理，所以对一个不存在的 key 进行 HMGET 操作将返回一个只带有 nil 值的表。
     *
     * @param dbIndex
     * @param key     hash表的标记key
     * @param fields  多个给定域的标识fields
     * @return 一个包含多个给定域的关联值的表，表值的排列顺序和给定域参数的请求顺序一样
     */
    public List<String> hmget(int dbIndex, String key, String... fields) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisSlavePool();
            jedis = jedisPool.getResource();
            if (0 != dbIndex) {
                jedis.select(dbIndex);
            }
            return jedis.hmget(key, fields);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis get fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /*** common operation ***/
    /**
     * 移除指定key和value,并返回删除个数
     *
     * @param key set中的key值
     * @return Long类型, 删除元素个数
     */
    public Long remove(String key) {
        return remove(0, key);
    }

    /*** common operation ***/
    /**
     * 移除指定key和value,并返回删除个数
     *
     * @param dbIndex
     * @param key     set中的key值
     * @return Long类型, 删除元素个数
     */
    public Long remove(int dbIndex, String key) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            jedis.select(dbIndex);
            return jedis.del(key);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis set fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 将 key 改名为 newkey 。当 key 和 newkey 相同，或者 key 不存在时，返回一个错误。
     * 当 newkey 已经存在时， RENAME 命令将覆盖旧值。
     *
     * @param oldkey 原有的key
     * @param newkey 新命名的key
     * @return 改名成功时提示 OK ，失败时候返回一个错误。
     */
    public String rename(String oldkey, String newkey) {
        return rename(0, oldkey, newkey);
    }

    /**
     * 将 key 改名为 newkey 。当 key 和 newkey 相同，或者 key 不存在时，返回一个错误。
     * 当 newkey 已经存在时， RENAME 命令将覆盖旧值。
     *
     * @param dbIndex
     * @param oldkey  原有的key
     * @param newkey  新命名的key
     * @return 改名成功时提示 OK ，失败时候返回一个错误。
     */
    public String rename(int dbIndex, String oldkey, String newkey) {

        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (0 != dbIndex) {
                jedis.select(dbIndex);
            }
            return jedis.rename(oldkey, newkey);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis set fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 判断当前key是否已存在
     *
     * @param key set中的key值
     * @return 存在则返回true 否则返回false
     */
    public boolean exists(String key) {
        return exists(0, key);
    }

    /**
     * 判断当前key是否已存在
     *
     * @param dbIndex
     * @param key     set中的key值
     * @return 存在则返回true 否则返回false
     */
    public boolean exists(int dbIndex, String key) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisSlavePool();
            jedis = jedisPool.getResource();
            if (0 != dbIndex) {
                jedis.select(dbIndex);
            }
            return jedis.exists(key);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis get fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 使用客户端向 Redis 服务器发送一个 PING 通常用于测试与服务器的连接是否仍然生效，或者用于测量延迟值。
     *
     * @return 如果服务器运作正常的话，会返回一个 PONG,否则抛出异常
     */
    public String ping() {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisSlavePool();
            jedis = jedisPool.getResource();

            return jedis.ping();
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis get fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /*** advanced operation ***/
    /**
     * 监视一个(或多个) key ，如果在事务执行之前这个(或这些) key 被其他命令所改动，那么事务将被打断。
     *
     * @param key set中的key值
     */
    public void watch(String key) {
        watch(0, key);
    }

    /**
     * 监视一个(或多个) key ，如果在事务执行之前这个(或这些) key 被其他命令所改动，那么事务将被打断。
     *
     * @param dbIndex
     * @param key     set中的key值
     */
    public void watch(int dbIndex, String key) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (0 != dbIndex) {
                jedis.select(dbIndex);
            }
            jedis.watch(key);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis set fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 进行事务处理
     *
     * @param transactionAction 事务对象
     * @return 事务提交后, 返回内容List
     */
    public List<Object> doTransaction(TransactionAction transactionAction) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();

            Transaction transaction = jedis.multi();
            transactionAction.execute(transaction);
            return transaction.exec();
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis set fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 通过使用管道，执行相关操作
     *
     * @param piplineAction 管道对象
     */
    public void doPipline(PiplineAction piplineAction) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();

            Pipeline pipline = jedis.pipelined();
            piplineAction.execute(pipline);
            pipline.sync();
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis set fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * @param piplineAction 管道对象
     * @return 管道操作返回的结果, List集合
     */
    public List<Object> doPiplineAndReturn(PiplineAction piplineAction) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();

            Pipeline pipline = jedis.pipelined();
            piplineAction.execute(pipline);
            return pipline.syncAndReturnAll();
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis set fail", e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * redis中进行管道相关操作时，实现此接口
     *
     * @author duowan-PC
     */
    public interface PiplineAction {
        void execute(Pipeline pipline);
    }

    /**
     * 数据库操作事务对象接口,在redis中进行事务相关的操作时，实现此接口
     *
     * @author duowan-PC
     */
    public interface TransactionAction {
        void execute(Transaction transaction);
    }

    /**
     * 查找所有符合给定模式 pattern 的 key
     *
     * @param dbIndex
     * @param pattern 正则表达式
     * @return
     */
    public Set<String> keys(int dbIndex, String pattern) {
        Set<String> result = new HashSet<>();
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisSlavePool();
            jedis = jedisPool.getResource();
            if (0 != dbIndex) {
                jedis.select(dbIndex);
            }
            result = jedis.keys(pattern);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis keys fail", e);
        } finally {
            jedisClose(jedis);
        }
        return result;
    }

    /**
     * 往列表头插入元素
     *
     * @param key    列表的KEY值 创建一个KEY(如果不存在) / 向KEY的列表中添加值
     * @param values 添加到列表中的值 一个或多个
     * @return 列表的长度
     */
    public Long lpush(final String key, final String... values) {
        return lpush(0, key, values);
    }

    /**
     * 往列表头插入元素
     *
     * @param dbIndex 一般都是 0
     * @param key     列表的KEY值 创建一个KEY(如果不存在) / 向KEY的列表中添加值
     * @param values  添加到列表中的值 一个或多个
     * @return 列表的长度
     */
    public Long lpush(final int dbIndex, final String key, final String... values) {
        Long result = 0L;
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (0 != dbIndex) {
                jedis.select(dbIndex);
            }
            result = jedis.lpush(key, values);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis lpush fail", e);
        } finally {
            jedisClose(jedis);
        }
        return result;
    }

    /**
     * 往列表尾插入元素
     *
     * @param key    列表的KEY值 创建一个KEY(如果不存在) / 向KEY的列表中添加值
     * @param values 添加到列表中的值 一个或多个
     * @return 列表的长度
     */
    public Long rpush(final String key, final String... values) {
        return rpush(0, key, values);
    }

    /**
     * 往列表尾插入元素
     *
     * @param dbIndex 一般都是 0
     * @param key     列表的KEY值 创建一个KEY(如果不存在) / 向KEY的列表中添加值
     * @param values  添加到列表中的值 一个或多个
     * @return 列表的长度
     */
    public Long rpush(final int dbIndex, final String key, final String... values) {
        Long result = 0L;
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (0 != dbIndex) {
                jedis.select(dbIndex);
            }
            result = jedis.rpush(key, values);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis lpush fail", e);
        } finally {
            jedisClose(jedis);
        }
        return result;
    }

    /**
     * 返回存储在 KEY 中的列表元素，开始和结束索引都是从 0 开始的。 索引为0 代表列表的第一个元素，1 代表第二个元素 以此类推。
     * <p>
     * 例如：LRANGE foobar 0 2 就会返回列表名为：foobar的前3个元素。
     * <p>
     * 开始 和 结束索引也可以为负数，负数的值表明与列表结尾的距离。例如： -1 表示列表的最后一个元素 -2表示倒数第2个元素 以此类推。
     * <p>
     * <b>RANG函数在多种语言中的一致性</b>
     * <p>
     * 注意：如果你有一个列表，它存储的值是从 0 到 100，LRANGE 0 10 会返回 11 个元素，也就是说最右边的索引元素会被包含进来。
     * 这可能与你选择的编程语言中与范围选择相关的函数不一致。(想一下 Ruby,Python中的range())
     * <p>
     * <b>超出索引</b>
     * <p>
     * 索引超出列表范围并不会产生错误：如果start的值超出了列表的最后一个元素索引值 或者 start > end ，会返回一个空列表。如果end 的值超出了列表的结尾。
     * Redis会把它当做列表的最后一个元素的索引来对待。
     * <p>
     * 时间复杂度：O(start+n) n为要查询的范围 即 end - start。
     *
     * @param key   列表名称
     * @param start 查询起始索引
     * @param end   查询结束索引
     * @return 返回列表从start到end索引之间的元素，包含两端
     */
    public List<String> lrange(final String key, final long start, final long end) {
        return lrange(0, key, start, end);
    }

    /**
     * 返回存储在 KEY 中的列表元素，开始和结束索引都是从 0 开始的。 索引为0 代表列表的第一个元素，1 代表第二个元素 以此类推。
     * <p>
     * 例如：LRANGE foobar 0 2 就会返回列表名为：foobar的前3个元素。
     * <p>
     * 开始 和 结束索引也可以为负数，负数的值表明与列表结尾的距离。例如： -1 表示列表的最后一个元素 -2表示倒数第2个元素 以此类推。
     * <p>
     * <b>RANG函数在多种语言中的一致性</b>
     * <p>
     * 注意：如果你有一个列表，它存储的值是从 0 到 100，LRANGE 0 10 会返回 11 个元素，也就是说最右边的索引元素会被包含进来。
     * 这可能与你选择的编程语言中与范围选择相关的函数不一致。(想一下 Ruby,Python中的range())
     * <p>
     * <b>超出索引</b>
     * <p>
     * 索引超出列表范围并不会产生错误：如果start的值超出了列表的最后一个元素索引值 或者 start > end ，会返回一个空列表。如果end 的值超出了列表的结尾。
     * Redis会把它当做列表的最后一个元素的索引来对待。
     * <p>
     * 时间复杂度：O(start+n) n为要查询的范围 即 end - start。
     *
     * @param dbIndex
     * @param key     列表名称
     * @param start   查询起始索引
     * @param end     查询结束索引
     * @return 返回列表从start到end索引之间的元素，包含两端
     */
    public List<String> lrange(final int dbIndex, final String key, final long start, final long end) {
        List<String> result = new ArrayList<>();
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisSlavePool();
            jedis = jedisPool.getResource();
            if (0 != dbIndex) {
                jedis.select(dbIndex);
            }
            result = jedis.lrange(key, start, end);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis lrange fail", e);
        } finally {
            jedisClose(jedis);
        }
        return result;
    }

    /**
     * 设置 KEY为 @key 的列表在索引@index处的值为@value
     * <p>
     * 索引超出列表范围[ 0,len-1]会报错
     * <p>
     * 与其他的LIST命令相似，索引也支持负数。 －1表示最后一个元素 -2表示倒数第二个元素，以此类推
     * <p>
     * <b>时间复杂度:</b>
     * <p>
     * O(N) (N为列表的长度), 设置表头或表尾元素的时间复杂度为：O(1).
     *
     * @param key   列表的KEY值
     * @param index 列表索引
     * @param value 要设置的新值
     * @return 操作结果
     */
    public String lset(final String key, final long index, final String value) {
        return lset(0, key, index, value);
    }

    /**
     * 设置 KEY为 @key 的列表在索引@index处的值为@value
     * <p>
     * 索引超出列表范围[ 0,len-1]会报错
     * <p>
     * 与其他的LIST命令相似，索引也支持负数。 －1表示最后一个元素 -2表示倒数第二个元素，以此类推
     * <p>
     * <b>时间复杂度:</b>
     * <p>
     * O(N) (N为列表的长度), 设置表头或表尾元素的时间复杂度为：O(1).
     *
     * @param key   列表的KEY值
     * @param index 列表索引
     * @param value 要设置的新值
     * @return 操作结果
     */
    public String lset(final int dbIndex, final String key, final long index, final String value) {
        String result = "";
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (0 != dbIndex) {
                jedis.select(dbIndex);
            }
            result = jedis.lset(key, index, value);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis lpush fail", e);
        } finally {
            jedisClose(jedis);
        }
        return result;
    }

    /**
     * 修剪一个已经存在的列表 它只会保留指定范围内的元素。开始和结束索引都是从0开始的。0是列表的第一个元素的索引，1是第二个，以此类推
     * <p>
     * 例如：LTRIM foobar 0 2 会修改存储KEY为 foobar 列表中的元素，使其只保留列表的前3个元素
     * <p>
     * 开始 和 结束索引也可以为负数，负数的值表明与列表结尾的距离。例如： -1 表示列表的最后一个元素 -2表示倒数第2个元素 以此类推。
     * <p>
     * <b>超出索引</b>
     * <p>
     * 索引超出列表范围并不会产生错误：如果start的值超出了列表的最后一个元素索引值 或者 start > end ，会返回一个空列表。如果end 的值超出了列表的结尾。
     * Redis会把它当做列表的最后一个元素的索引来对待。
     * <p>
     * <p>
     * Time complexity: O(n) (列表长度 - 修剪范围)
     *
     * @param key   列表的KEY值
     * @param start 开始索引
     * @param end   结束索引
     * @return 成功返回 "OK"
     */
    public String ltrim(final String key, final long start, final long end) {
        return ltrim(0, key, start, end);
    }

    /**
     * 修剪一个已经存在的列表 它只会保留指定范围内的元素。开始和结束索引都是从0开始的。0是列表的第一个元素的索引，1是第二个，以此类推
     * <p>
     * 例如：LTRIM foobar 0 2 会修改存储KEY为 foobar 列表中的元素，使其只保留列表的前3个元素
     * <p>
     * 开始 和 结束索引也可以为负数，负数的值表明与列表结尾的距离。例如： -1 表示列表的最后一个元素 -2表示倒数第2个元素 以此类推。
     * <p>
     * <b>超出索引</b>
     * <p>
     * 索引超出列表范围并不会产生错误：如果start的值超出了列表的最后一个元素索引值 或者 start > end ，会返回一个空列表。如果end 的值超出了列表的结尾。
     * Redis会把它当做列表的最后一个元素的索引来对待。
     * <p>
     * <p>
     * Time complexity: O(n) (列表长度 - 修剪范围)
     *
     * @param dbIndex 默认为 0
     * @param key     列表的KEY值
     * @param start   开始索引
     * @param end     结束索引
     * @return 成功返回 "OK"
     */
    public String ltrim(final int dbIndex, final String key, final long start, final long end) {
        String result = "";
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (0 != dbIndex) {
                jedis.select(dbIndex);
            }
            result = jedis.ltrim(key, start, end);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis ltrim fail", e);
        } finally {
            jedisClose(jedis);
        }
        return result;
    }

    /**
     * 设置指定KEY的生存时间，如果KEY的生存时间被用完，服务器会自动将KEY删除。带有过期时间的KEY在Redis术语中被称为易失的KEY。
     * <p>
     * 从redis 2.1.3开始，你可以更新已经设置了生存时间的KEY。你也可以通过使用 persist 命令让一个设置了生存时间的KEY
     * 变成一个正常的KEY，取消生存时间限制。
     * <p>
     * 时间复杂度: O(1)
     *
     * @param key
     * @param seconds
     * @return 成功返回: 1. 设置失败 ｜ KEY不存在返回：0.
     * @see <ahref="http://code.google.com/p/redis/wiki/ExpireCommand">ExpireCommand</a>
     */
    public Long expire(final String key, final int seconds) {
        return expire(0, key, seconds);
    }

    /**
     * 设置指定KEY的生存时间，如果KEY的生存时间被用完，服务器会自动将KEY删除。带有过期时间的KEY在Redis术语中被称为易失的KEY。
     * <p>
     * 从redis 2.1.3开始，你可以更新已经设置了生存时间的KEY。你也可以通过使用 persist 命令让一个设置了生存时间的KEY
     * 变成一个正常的KEY，取消生存时间限制。
     * <p>
     * 时间复杂度: O(1)
     *
     * @param key
     * @param seconds
     * @return 成功返回: 1. 设置失败 ｜ KEY不存在返回：0.
     * @see <ahref="http://code.google.com/p/redis/wiki/ExpireCommand">ExpireCommand</a>
     */
    public Long expire(final int dbIndex, final String key, final int seconds) {
        Long result = 0L;
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (0 != dbIndex) {
                jedis.select(dbIndex);
            }
            result = jedis.expire(key, seconds);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis expire fail", e);
        } finally {
            jedisClose(jedis);
        }
        return result;
    }

    public Long del(int dbIndex, String key) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.del(key);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis del db[ " + dbIndex + "] key:" + key, e);
        } finally {
            jedisClose(jedis);
        }
    }

    public Long del(String key) {
        return del(0, key);
    }

    /**************************************增加 SortSet 相关支持 *******************************************/
    public Long zadd(int dbIndex, String key, Map<String, Double> scoreMembers) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.zadd(key, scoreMembers);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis del db[ " + dbIndex + "] key:" + key, e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 批量增加 已经存在的 members 其值会被覆盖为插入的值
     *
     * @param key
     * @param scoreMembers
     * @return
     */
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        return zadd(0, key, scoreMembers);
    }

    public Long zadd(int dbIndex, String key, double score, String member) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.zadd(key, score, member);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis del db[ " + dbIndex + "] key:" + key, e);
        } finally {
            if (jedis != null && jedisPool != null) {
                jedis.close();
            }
        }
    }

    public Long zadd(String key, double score, String member) {
        return zadd(0, key, score, member);
    }

    public Double zincrby(int dbIndex, String key, double score, String member) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.zincrby(key, score, member);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis del db[ " + dbIndex + "] key:" + key, e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 会返回增加后的 score 值
     *
     * @param key
     * @param score
     * @param member
     * @return
     */
    public Double zincrby(String key, double score, String member) {
        return zincrby(0, key, score, member);
    }

    public Set<Tuple> zrangeWithScores(int dbIndex, String key, long start, long end) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisSlavePool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.zrangeWithScores(key, start, end);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis del db[ " + dbIndex + "] key:" + key, e);
        } finally {
            if (jedis != null && jedisPool != null) {
                jedis.close();
            }
        }
    }


    /**
     * 按分数从小到大排序
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        return zrangeWithScores(0, key, start, end);
    }

    public Set<Tuple> zrevrangeWithScores(int dbIndex, String key, long start, long end) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisSlavePool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.zrevrangeWithScores(key, start, end);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis del db[ " + dbIndex + "] key:" + key, e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 按分数逆排 从大到小
     *
     * @param key
     * @return
     */
    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        return zrevrangeWithScores(0, key, start, end);
    }

    public Double zscore(int dbIndex, String key, String member) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisSlavePool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.zscore(key, member);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis del db[ " + dbIndex + "] key:" + key, e);
        } finally {
            if (jedis != null && jedisPool != null) {
                jedis.close();
            }
        }
    }

    /**
     * 获取指定成员的分数
     *
     * @param key
     * @param member
     * @return
     */
    public Double zscore(String key, String member) {
        return zscore(0, key, member);
    }

    public Long zrank(int dbIndex, String key, String member) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisSlavePool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.zrank(key, member);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis del db[ " + dbIndex + "] key:" + key, e);
        } finally {
            jedisClose(jedis);
        }
    }

    /**
     * 返回指定成员的排名 从小到大排序
     *
     * @param key
     * @param member
     * @return
     */
    public Long zrank(String key, String member) {
        return zrank(0, key, member);
    }

    public Long zrevrank(int dbIndex, String key, String member) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisSlavePool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.zrevrank(key, member);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis del db[ " + dbIndex + "] key:" + key, e);
        } finally {
            if (jedis != null && jedisPool != null) {
                jedis.close();
            }
        }
    }

    /**
     * 返回指定成员的排名 从大到小排序
     *
     * @param key
     * @param member
     * @return
     */
    public Long zrevrank(String key, String member) {
        return zrevrank(0, key, member);
    }

    public Long zrem(int dbIndex, String key, String... members) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.zrem(key, members);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis del db[ " + dbIndex + "] key:" + key, e);
        } finally {
            jedisClose(jedis);
        }
    }


    /**
     * 批量移除成员
     *
     * @param key
     * @param members
     * @return
     */
    public Long zrem(String key, String... members) {
        return zrem(0, key, members);
    }


    public Long zremrangeByRank(int dbIndex, String key, long start, long end) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.zremrangeByRank(key, start, end);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis del db[ " + dbIndex + "] key:" + key, e);
        } finally {
            if (jedis != null && jedisPool != null) {
                jedis.close();
            }
        }
    }


    /**
     * 移除指定排名内的成员 排名按分数值从小到大排
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Long zremrangeByRank(String key, long start, long end) {
        return zremrangeByRank(0, key, start, end);
    }

    /****************************************** 消息分发 ***************************************************/
    public Long publish(int dbIndex, String channel, String message) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            return jedis.publish(channel, message);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis del db[ " + dbIndex + "] channel:" + channel, e);
        } finally {
            jedisClose(jedis);
        }
    }


    /**
     * 发送消息 message 到指定的 channel
     *
     * @param channel
     * @param message
     * @return
     */
    public Long publish(String channel, String message) {
        return publish(0, channel, message);
    }

    public void subscribe(int dbIndex, JedisPubSub jedisPubSub, String... channels) {
        Jedis jedis = null;
        JedisPool jedisPool = null;
        try {
            jedisPool = getJedisMasterPool();
            jedis = jedisPool.getResource();
            if (dbIndex != 0) {
                jedis.select(dbIndex);
            }
            jedis.subscribe(jedisPubSub, channels);
        } catch (Exception e) {
            exceptionHandler(jedisPool, jedis, e);
            jedis = null;
            throw new CsRedisRuntimeException("jedis del db[ " + dbIndex + "] channels:" + channels, e);
        } finally {
            if (jedis != null && jedisPool != null) {
                jedis.close();
            }
        }
    }


    /**
     * 订阅频道
     *
     * @param jedisPubSub
     * @param channels
     */
    public void subscribe(JedisPubSub jedisPubSub, String... channels) {
        subscribe(0, jedisPubSub, channels);
    }

}
