/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sparrow.cache.impl.redis;

import com.sparrow.cache.CacheDataNotFound;
import com.sparrow.cache.CacheHash;
import com.sparrow.constant.cache.KEY;
import com.sparrow.core.TypeConverter;
import com.sparrow.exception.CacheConnectionException;
import com.sparrow.json.Json;
import com.sparrow.utility.StringUtility;
import redis.clients.jedis.ShardedJedis;

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.ShardedJedisPipeline;

/**
 * Created by harry on 2018/1/26.
 */
public class RedisCacheHash extends AbstractCommand implements CacheHash {
    RedisCacheHash(RedisPool redisPool) {
        this.redisPool = redisPool;
    }

    @Override
    public Map<String, String> getAll(final KEY key) throws CacheConnectionException {
        return this.getAll(key, String.class, String.class);
    }

    @Override
    public <K, T> Map<K, T> getAll(final KEY key, final Class keyClazz, final Class clazz, final CacheDataNotFound<Map<K, T>> hook) {
        try {
            return redisPool.execute(new Executor<Map<K, T>>() {
                @Override
                public Map<K, T> execute(ShardedJedis jedis) throws CacheConnectionException {
                    Map<K, T> result = new HashMap<K, T>();
                    Map<String, String> map = jedis.hgetAll(key.key());
                    if (map == null || map.size() == 0) {
                        if (redisPool.getCacheMonitor() != null) {
                            redisPool.getCacheMonitor().penetrate(key);
                        }
                        result = hook.read(key);
                        RedisCacheHash.this.put(key, map);
                        return result;
                    }
                    TypeConverter keyConverter = new TypeConverter(keyClazz);
                    TypeConverter valueConverter = new TypeConverter(clazz);
                    for (String k : map.keySet()) {
                        if (StringUtility.isNullOrEmpty(map.get(k))) {
                            continue;
                        }
                        T t = (T) valueConverter.convert(map.get(k));
                        result.put((K) keyConverter.convert(k), t);
                    }
                    return result;
                }
            }, key);
        } catch (CacheConnectionException e) {
            if (redisPool.getCacheMonitor() != null) {
                redisPool.getCacheMonitor().penetrate(key);
            }
            return hook.read(key);
        }
    }

    @Override
    public <K, T> Map<K, T> getAll(final KEY key, final Class keyClazz, final Class clazz) throws CacheConnectionException {
        return redisPool.execute(new Executor<Map<K, T>>() {
            @Override
            public Map<K, T> execute(ShardedJedis jedis) throws CacheConnectionException {
                Map<K, T> result = new HashMap<K, T>();
                Map<String, String> map = jedis.hgetAll(key.key());
                TypeConverter valueConverter = new TypeConverter(clazz);
                TypeConverter keyTypeConverter = new TypeConverter(keyClazz);
                for (String k : map.keySet()) {
                    if (StringUtility.isNullOrEmpty(map.get(k))) {
                        continue;
                    }
                    T t = (T) valueConverter.convert(map.get(k));
                    result.put((K) keyTypeConverter.convert(k), t);
                }
                return result;
            }
        }, key);
    }

    @Override
    public Long getSize(final KEY key) throws CacheConnectionException {
        return redisPool.execute(new Executor<Long>() {
            @Override
            public Long execute(ShardedJedis jedis) throws CacheConnectionException {
                return jedis.hlen(key.key());
            }
        }, key);
    }

    @Override
    public String get(final KEY key, final String field) throws CacheConnectionException {
        return redisPool.execute(new Executor<String>() {
            @Override
            public String execute(ShardedJedis jedis) throws CacheConnectionException {
                return jedis.hget(key.key(), field);
            }
        }, key);
    }

    @Override
    public <T> T get(final KEY key, final String field, final Class clazz) throws CacheConnectionException {
        return redisPool.execute(new Executor<T>() {
            @Override
            public T execute(ShardedJedis jedis) throws CacheConnectionException {
                String value = jedis.hget(key.key(), field);
                if (StringUtility.isNullOrEmpty(value)) {
                    return null;
                }
                TypeConverter typeConverter = new TypeConverter(clazz);
                return (T) typeConverter.convert(value);
            }
        }, key);
    }

    @Override
    public Long put(final KEY key, final String field, final Object value) throws CacheConnectionException {
        return redisPool.execute(new Executor<Long>() {
            @Override
            public Long execute(ShardedJedis jedis) {
                TypeConverter typeConverter = new TypeConverter(String.class);
                return jedis.hset(key.key(), field, typeConverter.convert(value).toString());
            }
        }, key);
    }

    @Override
    public <K, T> Integer put(final KEY key, final Map<K, T> map) throws CacheConnectionException {
        return redisPool.execute(new Executor<Integer>() {
            @Override
            public Integer execute(ShardedJedis jedis) {
                ShardedJedisPipeline shardedJedisPipeline = jedis.pipelined();
                TypeConverter typeConverter = new TypeConverter(String.class);
                for (K k : map.keySet()) {
                    shardedJedisPipeline.hset(key.key(), k.toString(), typeConverter.convert(map.get(k)).toString());
                }
                shardedJedisPipeline.sync();
                return map.size();
            }
        }, key);
    }

    @Override
    public Long incrBy(final KEY key, final String field, final long count) throws CacheConnectionException {
        return redisPool.execute(new Executor<Long>() {
            @Override
            public Long execute(ShardedJedis jedis) {
                return jedis.hincrBy(key.key(), field, count);
            }
        }, key);
    }
}
