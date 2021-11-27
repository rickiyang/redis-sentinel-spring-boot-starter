package com.rickiyang.redis.redis.sentinel;

import redis.clients.jedis.JedisPubSub;

/**
 * @date: 2021/11/16 11:46 上午
 * @author: rickiyang
 * @Description:
 */
public class JedisPubSubAdapter extends JedisPubSub {
    @Override
    public void onMessage(String channel, String message) {
    }

    @Override
    public void onPMessage(String pattern, String channel, String message) {
    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {
    }

    @Override
    public void onPUnsubscribe(String pattern, int subscribedChannels) {
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {
    }
}
