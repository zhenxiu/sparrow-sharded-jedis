package com.sparrow.cache.impl.redis;

import com.sparrow.core.spi.JsonFactory;
import com.sparrow.json.Json;

/**
 * Created by harry on 2018/1/26.
 */
public class AbstractCommand {
    protected RedisPool redisPool;

    protected Json jsonProvider = JsonFactory.getProvider();
}
