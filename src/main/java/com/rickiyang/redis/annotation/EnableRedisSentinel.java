package com.rickiyang.redis.annotation;

import java.lang.annotation.*;

/**
 * @date: 2021/11/16 11:47 上午
 * @author: rickiyang
 * @Description:
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EnableRedisSentinel {
}

