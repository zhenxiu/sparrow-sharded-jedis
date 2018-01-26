package com.sparrow.cache.impl.redis;

import com.sparrow.cache.*;

/**
 * Created by harry on 2018/1/18.
 */
public class RedisCacheClient extends AbstractCommand implements CacheClient {
    private CacheString cacheString;
    private CacheSet cacheSet;
    private CacheOrderSet cacheOrderSet;
    private CacheHash cacheHash;
    private CacheKey cacheKey;
    private CacheList cacheList;

    public void setRedisPool(RedisPool redisPool) {
        this.redisPool = redisPool;
        this.cacheKey = new RedisCacheKey(redisPool);
        this.cacheString = new RedisCacheString(redisPool);
        this.cacheSet = new RedisCacheSet(redisPool);
        this.cacheOrderSet = new RedisCacheOrderSet(redisPool);
        this.cacheHash = new RedisCacheHash(redisPool);
        this.cacheList=new RedisCacheList(redisPool);
    }

    @Override
    public CacheString string() {
        return cacheString;
    }


    @Override
    public CacheSet set() {
        return cacheSet;
    }


    @Override
    public CacheOrderSet orderSet() {
        return cacheOrderSet;
    }

    @Override
    public CacheHash hash() {
        return cacheHash;
    }

    @Override
    public CacheKey key() {
        return cacheKey;
    }

    @Override
    public CacheList list() {
        return cacheList;
    }
}
