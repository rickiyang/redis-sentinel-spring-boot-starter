package com.rickiyang.redis.redis.sentinel;


import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

/**
 * @date: 2021/11/16 11:45 上午
 * @author: rickiyang
 * @Description: 方便获取从库的host port
 */
public class SlaveJedisPool extends JedisPool {

    private final HostAndPort hostAndPort;

    public SlaveJedisPool(final JedisPoolConfig poolConfig, final HostAndPort hostAndPort, final int timeout) {
        super(poolConfig, hostAndPort.getHost(), hostAndPort.getPort(), timeout, null, Protocol.DEFAULT_DATABASE, null);
        this.hostAndPort = hostAndPort;

    }

    public HostAndPort getHostAndPort() {
        return hostAndPort;
    }

}

