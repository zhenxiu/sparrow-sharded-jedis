package com.sparrow.cache.impl.redis;

import com.sparrow.cache.CacheKey;
import com.sparrow.constant.cache.KEY;
import com.sparrow.exception.CacheConnectionException;
import redis.clients.jedis.ShardedJedis;

/**
 * Created by harry on 2018/1/26.
 */
public class RedisCacheKey extends AbstractCommand implements CacheKey{
    RedisCacheKey(RedisPool redisPool){
        this.redisPool=redisPool;
    }

    @Override
    public Long expire(final KEY key, final Integer expire) throws CacheConnectionException {
        return redisPool.execute(new Executor<Long>() {
            @Override
            public Long execute(ShardedJedis jedis) {
                return jedis.expire(key.key(), expire);
            }
        },key);
    }

    @Override
    public Long delete(final KEY key) throws CacheConnectionException {
        return redisPool.execute(new Executor<Long>() {
            @Override
            public Long execute(ShardedJedis jedis) {
                return jedis.expireAt(key.key(), -1L);
            }
        },key);
    }

    @Override
    public Long expireAt(final KEY key, final Long expire) throws CacheConnectionException {
        return redisPool.execute(new Executor<Long>() {
            @Override
            public Long execute(ShardedJedis jedis) {
                return jedis.expireAt(key.key(), expire);
            }
        },key);
    }
}
