package com.ndustrialio.cacheutils;

import com.ndustrialio.cacheutils.RedisClient;
import org.joda.time.DateTime;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.List;
import java.util.Set;

/**
 * Created by jmhunt on 3/14/17.
 */
public class TimeScoreCache implements ITimeSeriesCache<String>
{
    public static final int DEFAULT_TTL = 60*60*5;

    // We will delete every DELETE_INTERVAL writes
    private static final int WRITE_DELETE_INTERVAL = 50;

    private int _writeCount = 0;


    protected RedisClient _client;

    protected int _ttl;

    public TimeScoreCache(RedisClient client, int ttl)
    {
        _ttl = ttl;
        _client = client;
    }

    public TimeScoreCache(RedisClient client)
    {
        this(client, DEFAULT_TTL);
    }


    public Value<String> getClosestAfter(final String key, final DateTime timestamp, final boolean inclusive)
    {
        final long hour = getHour(timestamp);

        final long offset = timestamp.getMillis()-hour;

        List<Object> data = (List<Object>)_client.execute(new JedisOperation()
        {
            @Override
            public Object execute(Jedis jedis)
            {
                String lowerBound = inclusive ? offset+"" : "("+offset;

                Pipeline p = jedis.pipelined();

                // Get data and update expiration
                p.zrangeByScore(key+":"+hour, lowerBound, "inf", 0, 1);
                if (_ttl != 0) p.expire(key+":"+hour, _ttl);

                return p.syncAndReturnAll();
            }
        });

        Set<String> response = (Set<String>)data.get(0);

        return response.isEmpty() ? null : getValue(hour, response.iterator().next());
    }

    public Value<String> getClosestBefore(final String key, final DateTime timestamp, final boolean inclusive)
    {
        final long hour = getHour(timestamp);

        final long offset = timestamp.getMillis()-hour;

        List<Object> data = (List<Object>)_client.execute(new JedisOperation()
        {
            @Override
            public Object execute(Jedis jedis)
            {
                String upperBound = inclusive ? offset+"" : "("+offset;

                Pipeline p = jedis.pipelined();

                // Get data and update expiration
                p.zrevrangeByScore(key+":"+hour, upperBound, "-inf", 0, 1);
                if (_ttl != 0) p.expire(key+":"+hour, _ttl);

                return p.syncAndReturnAll();
            }
        });

        Set<String> response = (Set<String>)data.get(0);

        return response.isEmpty() ? null : getValue(hour, response.iterator().next());
    }

    public Value<String> get(final String key, DateTime timestamp)
    {
        final long hour = getHour(timestamp);

        final long offset = timestamp.getMillis()-hour;

        List<Object> data = (List<Object>)_client.execute(new JedisOperation()
        {
            @Override
            public Object execute(Jedis jedis)
            {
                Pipeline p = jedis.pipelined();

                p.zrangeByScore(key+":"+hour, offset+"", offset+"", 0, 1);
                if (_ttl != 0) p.expire(key+":"+hour, _ttl);

                return p.syncAndReturnAll();
            }
        });

        Set<String> response = (Set<String>)data.get(0);


        return response.isEmpty() ? null : getValue(hour, response.iterator().next());
    }

    public void put(final String key, DateTime timestamp, String value)
    {
        final long hour = getHour(timestamp);

        final long offset = timestamp.getMillis()-hour;


        final String data = getData(offset, value);


        _client.execute(new JedisOperation()
        {
            @Override
            public Object execute(Jedis jedis)
            {
                Pipeline p = jedis.pipelined();

                // We want the insertion behavior of the sorted set
                // to act like it's based on time. This is O(log(n)), so its adviseable
                // to keep the set small ish.
                double score = (double)offset;

                // First of all, do a remove so we dont get more than one value
                // per timestamp
                p.zremrangeByScore(key+":"+hour, score, score);

                // Set new key
                p.zadd(key+":"+hour, score, data);
                // set expiration
                if (_ttl != 0) p.expire(key+":"+hour, _ttl);

                p.sync();
                return null;
            }
        });
    }


    private long getHour(DateTime timestamp)
    {
        return new DateTime(
                timestamp.getYear(),
                timestamp.getMonthOfYear(),
                timestamp.getDayOfMonth(),
                timestamp.getHourOfDay(),
                0,
                0,
                0).getMillis();
    }

    private Value<String> getValue(long hour, String json)
    {
        JSONObject obj = new JSONObject(json);

        return new Value<>(new DateTime(hour+obj.getLong("t")), obj.getString("v"));
    }

    private String getData(long offset, String value)
    {
        JSONObject obj = new JSONObject();

        obj.put("t", offset);
        obj.put("v", value);

        return obj.toString();
    }


}
