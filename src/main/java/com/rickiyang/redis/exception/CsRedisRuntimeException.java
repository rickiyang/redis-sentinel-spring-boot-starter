package com.rickiyang.redis.exception;

/**
 * @date: 2021/11/16 10:04 上午
 * @author: rickiyang
 * @Description: 操作Redis时的运行是异常
 */
public class CsRedisRuntimeException extends RuntimeException{

    private static final long serialVersionUID = 3100326186979666730L;

    public CsRedisRuntimeException() {
        super();
    }

    public CsRedisRuntimeException(String message) {
        super(message);
    }

    public CsRedisRuntimeException(Throwable cause) {
        super(cause);
    }

    public CsRedisRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
