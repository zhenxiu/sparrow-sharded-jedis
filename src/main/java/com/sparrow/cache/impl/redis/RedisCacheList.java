package com.sparrow.cache.impl.redis;

import com.sparrow.cache.CacheDataNotFound;
import com.sparrow.cache.CacheList;
import com.sparrow.constant.cache.KEY;
import com.sparrow.core.TypeConverter;
import com.sparrow.exception.CacheConnectionException;
import redis.clients.jedis.ShardedJedis;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by harry on 2018/1/26.
 */
public class RedisCacheList extends AbstractCommand implements CacheList {
    RedisCacheList(RedisPool redisPool) {
        this.redisPool = redisPool;
    }

    @Override
    public Long getSize(final KEY key) throws CacheConnectionException {
        return redisPool.execute(new Executor<Long>() {
            @Override
            public Long execute(ShardedJedis jedis) {
                return jedis.llen(key.key());
            }
        }, key);
    }

    @Override
    public Long add(final KEY key, final Object value) throws CacheConnectionException {
        return redisPool.execute(new Executor<Long>() {
            @Override
            public Long execute(ShardedJedis jedis) {
                TypeConverter typeConverter=new TypeConverter(String.class);
                return jedis.rpush(key.key(), typeConverter.convert(value).toString());
            }
        }, key);
    }


    @Override
    public Long add(final KEY key, final String... values) throws CacheConnectionException {
        return redisPool.execute(new Executor<Long>() {
            @Override
            public Long execute(ShardedJedis jedis) {
                return jedis.lpush(key.key(), values);
            }
        }, key);
    }

    @Override
    public <T> Integer add(final KEY key, final Iterable<T> values) throws CacheConnectionException {
        return redisPool.execute(new Executor<Integer>() {
            @Override
            public Integer execute(ShardedJedis jedis) {
                int i = 0;
                TypeConverter typeConverter=new TypeConverter(String.class);
                for (T value : values) {
                    if (value == null) {
                        continue;
                    }
                    i++;
                    jedis.lpush(key.key(),typeConverter.convert(value).toString());
                }
                return i;
            }
        }, key);
    }

    @Override
    public Long remove(final KEY key, final Object value) throws CacheConnectionException {
        return redisPool.execute(new Executor<Long>() {
            @Override
            public Long execute(ShardedJedis jedis) {
                return jedis.lrem(key.key(), 1L, value.toString());
            }
        }, key);
    }


    @Override
    public List<String> list(final KEY key) throws CacheConnectionException {
        return this.list(key, String.class);
    }

    @Override
    public <T> List<T> list(final KEY key, final Class clazz) throws CacheConnectionException {
        return redisPool.execute(new Executor<List<T>>() {
            @Override
            public List<T> execute(ShardedJedis jedis) throws CacheConnectionException {
                List<String> list = jedis.lrange(key.key(), 0,- 1);
                List<T> tList = new ArrayList<T>(list.size());
                for (String s : list) {
                    tList.add((T) RedisCacheList.this.jsonProvider.parse(s,clazz));
                }
                return tList;
            }
        }, key);
    }


    @Override
    public List<String> list(final KEY key, CacheDataNotFound<List<String>> hook) {
        return this.list(key, String.class, hook);
    }

    @Override
    public <T> List<T> list(final KEY key, final Class clazz, final CacheDataNotFound<List<T>> hook) {
        try {
            return redisPool.execute(new Executor<List<T>>() {
                @Override
                public List<T> execute(ShardedJedis jedis) throws CacheConnectionException {
                    List<String> list = jedis.lrange(key.key(), 0, - 1);
                    List<T> typeList =null;
                    if (list == null || list.size() == 0) {
                        typeList=hook.read(key);
                        RedisCacheList.this.add(key,list);
                        return typeList;
                    }
                    typeList =  new ArrayList<T>(list.size());
                    for (String s : list) {
                        typeList.add((T)RedisCacheList.this.jsonProvider.parse(s,clazz));
                    }
                    return typeList;
                }
            }, key);
        } catch (CacheConnectionException e) {
            return hook.read(key);
        }
    }
}
