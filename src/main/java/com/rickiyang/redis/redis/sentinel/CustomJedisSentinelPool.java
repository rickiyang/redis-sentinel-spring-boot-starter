package com.rickiyang.redis.redis.sentinel;


import com.rickiyang.redis.util.Json;
import com.rickiyang.redis.util.RedisUtils;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @date: 2021/11/16 11:44 上午
 * @author: rickiyang
 * @Description:
 */
public class CustomJedisSentinelPool extends JedisPool {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(CustomJedisSentinelPool.class);

    protected JedisPoolConfig poolConfig;

    protected int timeout = Protocol.DEFAULT_TIMEOUT;

    protected String password;

    protected int database = Protocol.DEFAULT_DATABASE;

    protected static final String MASTER_PREFIX = "master";

    protected static final String SLAVE_PREFIX = "slave";

    protected Set<MasterListener> masterListeners = new HashSet<>();

    protected SlavesChecker slaveChecker = null;

    protected Map<String, ArrayList<HostAndPort>> sentinelsMap = new ConcurrentHashMap<>();

    private Executor executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private volatile HostAndPort currentHostMaster;

    private CopyOnWriteArrayList<SlaveJedisPool> availableSlaves = new CopyOnWriteArrayList<>();

    private CopyOnWriteArrayList<HostAndPort> unavailableSlaves = new CopyOnWriteArrayList<>();

    private AtomicLong lastLoadTimestamp = new AtomicLong();

    public CustomJedisSentinelPool(String masterName, Set<String> sentinels, final JedisPoolConfig poolConfig) {
        this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE);
    }

    public CustomJedisSentinelPool(String masterName, Set<String> sentinels) {
        this(masterName, sentinels, new JedisPoolConfig(), Protocol.DEFAULT_TIMEOUT, null, Protocol.DEFAULT_DATABASE);
    }

    public CustomJedisSentinelPool(String masterName, Set<String> sentinels, String password) {
        this(masterName, sentinels, new JedisPoolConfig(), Protocol.DEFAULT_TIMEOUT, password);
    }

    public CustomJedisSentinelPool(String masterName, Set<String> sentinels, final JedisPoolConfig poolConfig,
                                   int timeout, final String password) {
        this(masterName, sentinels, poolConfig, timeout, password, Protocol.DEFAULT_DATABASE);
    }

    public CustomJedisSentinelPool(String masterName, Set<String> sentinels, final JedisPoolConfig poolConfig,
                                   final int timeout) {
        this(masterName, sentinels, poolConfig, timeout, null, Protocol.DEFAULT_DATABASE);
    }

    public CustomJedisSentinelPool(String masterName, Set<String> sentinels, final JedisPoolConfig poolConfig,
                                   final String password) {
        this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, password);
    }

    public CustomJedisSentinelPool(String masterName, Set<String> sentinels, final JedisPoolConfig poolConfig,
                                   int timeout, final String password, final int database) {
        this.poolConfig = poolConfig;
        this.timeout = timeout;
        this.password = password;
        this.database = database;
        initSentinels(sentinels, masterName, timeout);
    }

    @Override
    public Jedis getResource() {
        Jedis jedis = super.getResource();
        jedis.setDataSource(this);
        return jedis;
    }

    @Override
    public void returnBrokenResource(final Jedis resource) {
        if (resource != null) {
            returnBrokenResourceObject(resource);
        }
    }

    @Override
    public void returnResource(final Jedis resource) {
        if (resource != null) {
            resource.resetState();
            returnResourceObject(resource);
        }
    }

    @Override
    public void destroy() {
        for (MasterListener m : masterListeners) {
            m.shutdown();
        }
        if (null != slaveChecker) {
            slaveChecker.shutdown();
        }

        if (availableSlaves != null && availableSlaves.size() > 0) {
            for (JedisPool pool : availableSlaves) {
                pool.close();
            }
            availableSlaves.clear();
        }
        if (null != unavailableSlaves) {
            unavailableSlaves.clear();
        }
        log.info("CustomJedisSentinelPool destroy...");
        super.destroy(); // close myself
    }

    public HostAndPort getCurrentHostMaster() {
        return currentHostMaster;
    }

    private void initMasterPool(HostAndPort master) {
        // 覆写equals，避免重复初始化master pool
        if (!master.equals(currentHostMaster)) {
            ArrayList<HostAndPort> ls = new ArrayList<>();
            ls.add(master);
            sentinelsMap.put(MASTER_PREFIX, ls);
            currentHostMaster = master;
            log.info("Created JedisPool to master at " + master);
            // 创建master pool
            initPool(poolConfig, new JedisFactory(master.getHost(), master.getPort(), timeout, password, database));
        }
    }

    private void initSalvePools(ArrayList<HostAndPort> slaves) {
        try {
            lock.writeLock().lock();
            boolean isSame = false;
            ArrayList<HostAndPort> list = sentinelsMap.get(SLAVE_PREFIX);
            if (list != null && slaves != null && slaves.containsAll(list) && list.containsAll(slaves)) {
                isSame = true;
            }
            if (isSame) {
                return;
            } else {
                sentinelsMap.put(SLAVE_PREFIX, slaves);
            }
            if (availableSlaves != null && availableSlaves.size() > 0) {
                for (JedisPool pool : availableSlaves) {
                    log.info("remove and destroy old jedisPool :{}", pool);
                    pool.close();
                }
                availableSlaves.clear();
            }
            if (unavailableSlaves != null && unavailableSlaves.size() > 0) {
                unavailableSlaves.clear();
            }
            for (HostAndPort hap : slaves) {
                if (RedisUtils.isAvailable(hap.getHost(), hap.getPort(), timeout)) {
                    availableSlaves.add(new SlaveJedisPool(poolConfig, hap, timeout));
                    log.info("reload new jedisPool host:{},port:{}", hap.getHost(), hap.getPort());
                } else {
                    unavailableSlaves.add(hap);
                    log.warn("relaod failed jedisPool host:{},port:{}", hap.getHost(), hap.getPort());
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public JedisPool getReaderPool() {
        try {
            lock.readLock().lock();
            int size = availableSlaves.size();
            if (size > 0) {
                return availableSlaves.get(ThreadLocalRandom.current().nextInt(size));
            } else {
                log.info("error: none slave pool can be aquired");
                return null;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    private Map<String, ArrayList<HostAndPort>> initSentinels(Set<String> sentinels, final String masterName,
                                                              int timeout) {
        Map<String, ArrayList<HostAndPort>> map = new HashMap<>();
        HostAndPort master = null;
        boolean running = true;
        log.info("start initSentinels...");
        outer:
        while (running) {
            log.info("Trying to find master from available Sentinels...");
            for (String sentinel : sentinels) {
                final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
                log.debug("Connecting to Sentinel " + hap);
                try {
                    @SuppressWarnings("resource")
                    Jedis jedis = new Jedis(hap.getHost(), hap.getPort(), timeout);
                    if (master == null) {
                        master = toHostAndPort(jedis.sentinelGetMasterAddrByName(masterName));
                        log.info("found Redis master at " + master);
                        ArrayList<HostAndPort> ls = new ArrayList<>();
                        ls.add(master);
                        sentinelsMap.put(MASTER_PREFIX, ls);
                        // 初始化masterPool
                        initMasterPool(master);
                        // 获取从服务器列表
                        ArrayList<HostAndPort> slaves = new ArrayList<>();
                        for (Map<String, String> slave : jedis.sentinelSlaves(masterName)) {
                            HostAndPort _slave = toHostAndPort(Arrays.asList(slave.get("name").split(":")));
                            slaves.add(_slave);
                            log.info("Found Redis Slave: " + Json.ObjToStr(slave));
                        }
                        // 初始化slavePool
                        initSalvePools(slaves);
                        lastLoadTimestamp.set(System.currentTimeMillis());

                        jedisClose(jedis);
                        break outer;
                    }
                } catch (JedisConnectionException e) {
                    log.warn("Cannot connect to sentinel running @ " + hap + ". Trying next one.");
                }
            }
            try {
                log.error("All sentinels down, cannot determine where is " + masterName
                        + " master is running... sleeping 1000ms.");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        log.info("Redis master running at " + master + ", starting Sentinel listeners...");
        for (String sentinel : sentinels) {
            final HostAndPort hap = toHostAndPort(Arrays.asList(sentinel.split(":")));
            MasterListener masterListener = new MasterListener(masterName, hap.getHost(), hap.getPort());
            masterListeners.add(masterListener);
            masterListener.setDaemon(true);
            masterListener.start();
        }

        slaveChecker = new SlavesChecker();
        slaveChecker.setDaemon(true);
        slaveChecker.start();

        return map;
    }

    private void reloadSlavePools(Jedis jedis, String masterName) {
        ArrayList<HostAndPort> slaves = new ArrayList<>();
        for (Map<String, String> slave : jedis.sentinelSlaves(masterName)) {
            HostAndPort subSlave = toHostAndPort(Arrays.asList(slave.get("name").split(":")));
            slaves.add(subSlave);
            log.info("reloadSlavePools: found Redis Slave: " + Json.ObjToStr(subSlave));
        }
        sentinelsMap.put(SLAVE_PREFIX, slaves);
        initSalvePools(slaves);
        jedisClose(jedis);
    }

    private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
        String host = getMasterAddrByNameResult.get(0);
        int port = Integer.parseInt(getMasterAddrByNameResult.get(1));
        return new HostAndPort(host, port);
    }

    protected class SlavesChecker extends Thread {

        protected AtomicBoolean running = new AtomicBoolean(false);

        @Override
        public void run() {
            running.set(true);
            while (running.get()) {
                try {
                    TimeUnit.SECONDS.sleep(30);// 写死30s？
                } catch (InterruptedException e) {
                    shutdown();
                    return;
                } catch (Exception e) {

                }
                long lastUpdate = lastLoadTimestamp.get();
                List<SlaveJedisPool> newUnavailable = new ArrayList<>();
                List<HostAndPort> newAvailable = new ArrayList<>();
                lock.readLock().lock();
                try {
                    if (availableSlaves.isEmpty() && unavailableSlaves.isEmpty()) {
                        continue;
                    }
                    for (SlaveJedisPool jp : availableSlaves) {
                        try (Jedis j = jp.getResource();) {
                            if (!RedisUtils.ping(j)) {
                                newUnavailable.add(jp);
                            }
                        } catch (Exception e) {
                            newUnavailable.add(jp);
                        }
                    }

                    for (HostAndPort hap : unavailableSlaves) {
                        if (RedisUtils.isAvailable(hap.getHost(), hap.getPort(), timeout)) {
                            newAvailable.add(hap);
                        }
                    }
                } finally {
                    lock.readLock().unlock();
                }

                if (newUnavailable.size() > 0 || newAvailable.size() > 0) {
                    lock.writeLock().lock();
                    if (lastUpdate != lastLoadTimestamp.get()) {
                        continue;
                    }
                    try {
                        if (!newUnavailable.isEmpty()) {
                            for (SlaveJedisPool jp : newUnavailable) {
                                unavailableSlaves.add(jp.getHostAndPort());
                                availableSlaves.remove(jp);
                                if (log.isDebugEnabled()) {
                                    log.debug(" remove unavailable jedis slave pool " + jp.getHostAndPort());
                                }
                            }
                        }

                        if (!newAvailable.isEmpty()) {
                            for (HostAndPort hap : newAvailable) {
                                availableSlaves.add(new SlaveJedisPool(poolConfig, hap, timeout));
                                unavailableSlaves.remove(hap);
                                if (log.isDebugEnabled()) {
                                    log.debug(" add available jedis slave pool " + hap);
                                }
                            }
                        }

                    } finally {
                        lock.writeLock().unlock();
                    }
                }
            }
        }

        public void shutdown() {
            try {
                log.info("Shutting down SlaveChecker ");
                running.set(false);
            } catch (Exception e) {
                log.error("Caught exception while shutting down: " + e.getMessage());
            }
        }

    }

    protected class MasterListener extends Thread {

        protected String masterName;
        protected String host;
        protected int port;
        protected long subscribeRetryWaitTimeMillis = 5000;
        protected Jedis j;
        protected AtomicBoolean running = new AtomicBoolean(false);

        protected MasterListener() {
        }

        public MasterListener(String masterName, String host, int port) {
            this.masterName = masterName;
            this.host = host;
            this.port = port;
        }

        public MasterListener(String masterName, String host, int port, long subscribeRetryWaitTimeMillis) {
            this(masterName, host, port);
            this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
        }

        @Override
        public void run() {
            running.set(true);
            while (running.get()) {
                try {
                    j = new Jedis(host, port);
                    j.subscribe(new JedisPubSubAdapter() {
                        @Override
                        public void onMessage(String channel, String message) {
                            log.info("Sentinel " + host + ":" + port + " published: " + message + ".");
                            String[] switchMasterMsg = message.split(" ");
                            if (switchMasterMsg.length > 3) {
                                if (masterName.equals(switchMasterMsg[0])) {
                                    HostAndPort hostAddress = toHostAndPort(
                                            Arrays.asList(switchMasterMsg[3], switchMasterMsg[4]));
                                    log.info("switch master and init pool at :{}", hostAddress);
                                    initMasterPool(hostAddress);
                                    executorService.execute(() -> reloadSlavePools(new Jedis(host, port), masterName));
                                } else {
                                    log.info("Ignoring message on +switch-master for master name " + switchMasterMsg[0]
                                            + ", our master name is " + masterName);
                                }
                            } else {
                                log.error("Invalid message received on Sentinel  host:" + port
                                        + " on channel +switch-master: " + message);
                            }
                        }
                    }, "+switch-master");
                } catch (JedisConnectionException e) {
                    if (running.get()) {
                        log.error("Lost connection to Sentinel at " + host + ":" + port
                                + ". Sleeping 5000ms and retrying.", e);
                        try {
                            Thread.sleep(subscribeRetryWaitTimeMillis);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    } else {
                        log.info("Unsubscribing from Sentinel at " + host + ":" + port);
                    }
                } finally {
                    jedisClose(j);
                }
            }
        }

        public void shutdown() {
            try {
                log.info("Shutting down listener on " + host + ":" + port);
                running.set(false);
                // This isn't good, the Jedis object is not thread safe
                j.disconnect();
            } catch (Exception e) {
                log.error("Caught exception while shutting down: " + e.getMessage());
            }
        }
    }


    private void jedisClose(Jedis jedis) {
        try {
            if (jedis != null) {
                jedis.close();
            }
        } catch (Exception e) {
            log.error("jedis close fail", e);
        }
    }
}
