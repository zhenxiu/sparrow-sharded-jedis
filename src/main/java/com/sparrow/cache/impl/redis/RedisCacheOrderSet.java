package com.sparrow.cache.impl.redis;

import com.sparrow.cache.CacheDataNotFound;
import com.sparrow.cache.CacheOrderSet;
import com.sparrow.constant.cache.KEY;
import com.sparrow.core.TypeConverter;
import com.sparrow.exception.CacheConnectionException;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.Tuple;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by harry on 2018/1/26.
 */
public class RedisCacheOrderSet extends AbstractCommand implements CacheOrderSet {
    RedisCacheOrderSet(RedisPool redisPool) {
        this.redisPool=redisPool;
    }

    @Override
    public Long getSize(final KEY key) throws CacheConnectionException {
        return redisPool.execute(new Executor<Long>() {
            @Override
            public Long execute(ShardedJedis jedis) {
                return jedis.zcard(key.key());
            }
        }, key);
    }

    @Override
    public Long add(final KEY key, final Object value, final double score) throws CacheConnectionException {
        return redisPool.execute(new Executor<Long>() {
            @Override
            public Long execute(ShardedJedis jedis) {
                TypeConverter typeConverter=new TypeConverter(String.class);
                return jedis.zadd(key.key(), score, typeConverter.convert(value).toString());
            }
        }, key);
    }


    @Override
    public Long remove(final KEY key, final Object value) throws CacheConnectionException {
        return redisPool.execute(new Executor<Long>() {
            @Override
            public Long execute(ShardedJedis jedis) {
                return jedis.zrem(key.key(), value.toString());
            }
        }, key);
    }

    @Override
    public Long remove(final KEY key, final Long from, final Long to) throws CacheConnectionException {
        return redisPool.execute(new Executor<Long>() {
            @Override
            public Long execute(ShardedJedis jedis) {
                return jedis.zremrangeByRank(key.key(), from, to);
            }
        }, key);
    }

    @Override
    public Double getScore(final KEY key, final Object value) throws CacheConnectionException {
        return redisPool.execute(new Executor<Double>() {
            @Override
            public Double execute(ShardedJedis jedis) {
                return jedis.zscore(key.key(), value.toString());
            }
        }, key);
    }

    @Override
    public Long getRank(final KEY key, final Object value) throws CacheConnectionException {
        return redisPool.execute(new Executor<Long>() {
            @Override
            public Long execute(ShardedJedis jedis) {
                return jedis.zrank(key.key(), value.toString());
            }
        }, key);
    }

    @Override
    public Map<String, Double> getAllWithScore(final KEY key) throws CacheConnectionException {
        return redisPool.execute(new Executor<Map<String, Double>>() {
            @Override
            public Map<String, Double> execute(ShardedJedis jedis) {
                Set<Tuple> tuples = jedis.zrevrangeWithScores(key.key(), 0, -1);
                Map<String, Double> scoreMap = new LinkedHashMap<String, Double>(tuples.size());
                for (Tuple tuple : tuples) {
                    scoreMap.put(tuple.getElement(), tuple.getScore());
                }
                return scoreMap;
            }
        }, key);
    }

    @Override
    public Integer putAllWithScore(final KEY key, final Map<String, Double> keyScoreMap) throws CacheConnectionException {
        return redisPool.execute(new Executor<Integer>() {
            @Override
            public Integer execute(ShardedJedis jedis) {
                for (String value : keyScoreMap.keySet()) {
                    jedis.zadd(key.key(), keyScoreMap.get(value), value);
                }
                return keyScoreMap.size();
            };
        }, key);
    }

    @Override
    public  Map<String, Double> getAllWithScore(final KEY key,final CacheDataNotFound<Map<String,Double>> hook) {
        try {
            return redisPool.execute(new Executor<Map<String, Double>>() {
                @Override
                public Map<String, Double> execute(ShardedJedis jedis) {
                    Map<String, Double> scoreMap = null;
                    Set<Tuple> tuples = jedis.zrevrangeWithScores(key.key(), 0, -1);
                    if (tuples == null || tuples.size() == 0) {
                        scoreMap = hook.read(key);
                        try {
                            RedisCacheOrderSet.this.putAllWithScore(key,scoreMap);
                        } catch (CacheConnectionException ignore) {
                        }
                        return scoreMap;
                    }
                    scoreMap = new LinkedHashMap<String, Double>(tuples.size());
                    for (Tuple tuple : tuples) {
                        scoreMap.put(tuple.getElement(), tuple.getScore());
                    }
                    return scoreMap;
                }
            }, key);
        } catch (CacheConnectionException e) {
            return hook.read(key);
        }
    }
}
