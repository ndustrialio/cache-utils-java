package com.ndustrialio.cacheutils;


import org.joda.time.DateTime;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;


public class TimeHashingCache implements ITimeSeriesCache<String>
{


    // Expiration time for a key in seconds.  Applies to the whole key..
    // can't expire a particular field
	public static final int DEFAULT_TTL = 60*60*5;

	protected RedisClient _client;

	protected int _ttl;


	public TimeHashingCache(RedisClient client, int ttl)
	{
		_ttl = ttl;
		_client = client;
	}

	public TimeHashingCache(RedisClient client)
	{
		this(client, DEFAULT_TTL);
	}



	public Value<String> getClosestAfter(final String key, DateTime timestamp, boolean inclusive)
	{
		final long hour = getHour(timestamp);

		Map<String, String> cacheResponse = clientGetAll(key+":"+hour);

		if(cacheResponse.isEmpty())
		{
			// Nothin here
			return null;
		}

		TreeMap<DateTime, String> resultMap = new TreeMap<>();


		for(Map.Entry<String, String> e : cacheResponse.entrySet())
		{
			resultMap.put(new DateTime(hour+Long.parseLong(e.getKey())), e.getValue());
		}

        Map.Entry<DateTime, String> e;


        if (inclusive)
        {
            // ceilingEntry() includes the specified key
            e = resultMap.ceilingEntry(timestamp);
        } else
        {
            // higherEntry() does not
            e = resultMap.higherEntry(timestamp);
        }

        return new Value<>(e.getKey(), e.getValue());

	}

	public Value<String> getClosestBefore(String key, DateTime timestamp, boolean inclusive)
	{
		final long hour = getHour(timestamp);


		Map<String, String> cacheResponse = clientGetAll(key+":"+hour);

		if(cacheResponse.isEmpty())
		{
			// Nothin here
			return null;
		}

		TreeMap<DateTime, String> resultMap = new TreeMap<>();


		for(Map.Entry<String, String> e : cacheResponse.entrySet())
		{
			resultMap.put(new DateTime(hour+Long.parseLong(e.getKey())), e.getValue());
		}

        Map.Entry<DateTime, String> e;


        if (inclusive)
        {
            // floorEntry() includes the specified key
            e = resultMap.floorEntry(timestamp);
        } else
        {
            // lowerEntry() does not
            e = resultMap.lowerEntry(timestamp);
        }

		return new Value<>(e.getKey(), e.getValue());
	}


	@SuppressWarnings("unchecked")
	public List<Value<String>> get(DateTime timestamp, final String... keys)
    {
        final long hour = getHour(timestamp);

        final long offset = timestamp.getMillis()-hour;

        List<Object> values = (List<Object>)_client.execute((handle)-> {
            Pipeline p = handle.pipelined();

            for (String key: keys)
            {
                p.hget(key+":"+hour, offset+"");
            }


            return p.syncAndReturnAll();
        });

        return values.stream()
				.map(Optional::ofNullable)
				.map(value -> value.map(v ->new Value<>(timestamp, v.toString())).orElse(null))
                .collect(Collectors.toList());
    }


	public Value<String> get(final String key, DateTime timestamp)
	{
		final long hour = getHour(timestamp);

		final long offset = timestamp.getMillis()-hour;

		String value = (String)_client.execute(new JedisOperation()
		{
			@Override
			public Object execute(Jedis jedis)
			{
				return jedis.hget(key+":"+hour, offset+"");
			}
		});

		return new Value<>(timestamp, value);
	}

	public void put(final String key, DateTime timestamp, final String value)
	{
		final long hour = getHour(timestamp);

		final long offset = timestamp.getMillis()-hour;

		_client.execute(new JedisOperation()
		{
			@Override
			public Object execute(Jedis jedis)
			{
                Pipeline p = jedis.pipelined();

				p.hset(key+":"+hour, offset+"", value);
				if (_ttl != 0)
                {
                    p.expire(key+":"+hour, _ttl);
                }

                p.sync();

				return null;
			}
		});
	}


	private Map<String, String> clientGetAll(final String key)
	{
		return (Map<String, String>)_client.execute(new JedisOperation()
		{
			@Override
			public Object execute(Jedis jedis)
			{
				return jedis.hgetAll(key);
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
}
