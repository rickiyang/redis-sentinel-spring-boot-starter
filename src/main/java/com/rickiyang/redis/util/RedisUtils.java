package com.rickiyang.redis.util;

import com.rickiyang.redis.exception.CsRedisRuntimeException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @date: 2021/11/16 10:03 上午
 * @author: rickiyang
 * @Description:
 */

public class RedisUtils {

    private static Pattern roleP = Pattern.compile("role:(\\w+)");

    private static Pattern connectedClientsP = Pattern.compile("connected_clients:(\\d+)");


    /**
     * 测试redis是否可以连接
     *
     * @param host
     * @param port
     * @param timeout
     * @return
     */
    public static boolean isAvailable(String host, int port, int timeout) {
        try {
            Jedis jedis = new Jedis(host, port, timeout);
            jedis.connect();
            jedis.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 从jedis返回的info信息中获取connected client 数量
     * </br>
     * 如果info为空，或者匹配信息不到，则返回 0
     *
     * @param info redis服务器的相关配置信息
     * @return connected client 数量
     */
    public static int getConnectedClientNum(String info) {
        if (info == null || "".equals(info)) {
            return 0;
        }
        Matcher m = connectedClientsP.matcher(info);
        if (m.find()) {
            return Integer.valueOf(m.group(1));
        }
        return 0;
    }

    /**
     * 从jedis返回的info信息中分析 该redis实例是否是 master
     * </br>
     * 如果info为空，或者匹配信息不到，则返回 false
     *
     * @param info redis服务器的相关配置信息
     * @return boolean
     */
    public static boolean isMaster(String info) {
        if (info == null || "".equals(info)) {
            return false;
        }
        Matcher m = roleP.matcher(info);
        if (m.find()) {
            String isMaster = (m.group(1));
            return "master".equals(isMaster) ? true : false;
        }
        return false;
    }

    /**
     * @param serverInfo 格式如 ip:port:password:timeout
     * @return String数组，分别为ip,port，password,timeout
     */
    public static String[] parseServerInfo(String serverInfo) {
        if (serverInfo == null || "".equals(serverInfo)) {
            throw new CsRedisRuntimeException("invalid param, param couldn't be blank");
        }
        String[] str = serverInfo.split(":");
        for (int i = 0; i < str.length; i++) {
            if ("".equals(str[i])) {
                str[i] = null;
            }
        }
        if (str.length < 2) {
            throw new CsRedisRuntimeException(
                    "invalid param, param should like ip:port:password:timeout, if not exists password, you can use ip:port::");
        }
        String[] result = new String[4];
        result[0] = str[0];
        result[1] = str[1];
        if (str.length == 3) {
            result[2] = str[2];
        }
        if (str.length == 4) {
            result[2] = str[2];
            result[3] = str[3];
        }
        return result;
    }

    /**
     * ip支持用 "|" 分割多个。对于多IDC接入的服务器，填写不同网络类型的IP，按顺序去尝试可用的网络
     *
     * @param ip
     * @return
     */
    public static String[] parseServerIp(String ip) {
        String[] str = null;
        if (ip.contains("|")) {
            str = ip.split("\\|");
        } else {
            str = new String[]{ip};
        }
        return str;
    }

    /**
     * 判断jedis依赖的commons-pool版本。
     * jedis2.4.2后，使用的是commons-pool2通过特征字段获取commons-pool的版本，默认是返回1。commons-pool2返回2
     *
     * @return
     */
    public static int versionOfCommonsPool() {
        int version = 1;
        Class<?> jp = JedisPoolConfig.class;
        try {
            jp.getMethod("getMaxActive");
        } catch (NoSuchMethodException e) {
            version = 2;
        }
        return version;
    }

    /**
     * 通过反射创建JedisPool实例，用于兼容新老版本的Jedis
     *
     * @param config
     * @param ip
     * @param port
     * @param timeout
     * @param password
     * @return
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     */
    public static JedisPool getJedisPool(JedisPoolConfig config, String ip, int port, int timeout, String password)
            throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
        JedisPool pool = null;
        Class<JedisPool> pClz = JedisPool.class;
        Constructor<JedisPool> constructor = pClz.getConstructor(JedisPoolConfig.class.getSuperclass(), String.class,
                int.class, int.class, String.class);
        pool = constructor.newInstance(config, ip, port, timeout, password);
        return pool;
    }

    public static boolean ping(Jedis jedis) {
        try {
            if ("PONG".equals(jedis.ping())) {
                return true;
            }
        } catch (Exception e) {

        }
        return false;
    }
}

