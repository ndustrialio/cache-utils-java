package com.ndustrialio.cacheutils;

import com.ndustrialio.contxt.BaseConfiguredComponent;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Map;

/**
 * Created by jmhunt on 2/15/17.
 */
public class RedisClient extends BaseConfiguredComponent
{

    protected JedisPool _jedisPool;

    protected int _database;

    public RedisClient(Map conf, int db)
    {
        this.setConf(conf);

        _database = db;

        _jedisPool = new JedisPool((String)this.getConfigurationValue("redis_host"));

    }

    public Object execute(JedisOperation op)
    {
        Object ret = null;

        try (Jedis jedis = _jedisPool.getResource())
        {
            jedis.select(_database);

            //LOG.info(_jedisPool.getRemainingConnections() + " remaining connections after get");

            //LOG.info("Executing JedisOperation..");
            // Execute operation and get results
            ret = op.execute(jedis);

            //LOG.info("Finished executing jedis operation");
        }

        return ret;
    }

}
