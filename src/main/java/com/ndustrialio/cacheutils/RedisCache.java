package com.ndustrialio.cacheutils;

import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.Set;

public class RedisCache implements Cache<String>
{

	private RedisClient _client;

	private int _timeout;
	

    public RedisCache(Map conf, int dbNumber)
    {
        _timeout = 0;

        _client = new RedisClient(conf , dbNumber);
    }

	public RedisCache(Map conf, int dbNumber, int timeout)
	{

		_timeout = timeout;

		_client = new RedisClient(conf , dbNumber);


	}

	public RedisClient getClient()
	{
		return _client;
	}
	
	public boolean hashExists(final String hash)
	{
		return (Boolean)_client.execute(new JedisOperation()
		{
			@Override
			public Object execute(Jedis jedis)
			{
				return jedis.exists(hash);
			}
		});
		
	}
	
	public void deleteKey(final String key)
	{
		_client.execute(new JedisOperation()
		{
			
			@Override
			public Object execute(Jedis jedis)
			{
				jedis.del(key);
				
				return null;
			}
		});
	}
	
	@SuppressWarnings("unchecked")
	public Set<String> getKeys(final String pattern)
	{
		return (Set<String>)_client.execute(new JedisOperation()
		{
			
			@Override
			public Object execute(Jedis jedis)
			{
				// TODO Auto-generated method stub
				return jedis.keys(pattern);
			}
		});
	}
	
	public boolean keyExists(final String hash, final String key)
	{
		return (Boolean)_client.execute((new JedisOperation()
		{
			
			@Override
			public Object execute(Jedis jedis)
			{
				return jedis.hexists(hash, key);
			}
		}));
		
	}
	
	public String get(final String hash, final String key)
	{
		return (String)_client.execute((new JedisOperation()
		{
			
			@Override
			public Object execute(Jedis jedis)
			{
				// TODO Auto-generated method stub
				return jedis.hget(hash, key);
			}
		}));
		
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, String> getAll(final String hash)
	{		
		return (Map<String, String>)_client.execute((new JedisOperation()
		{
			
			@Override
			public Object execute(Jedis jedis)
			{
				// TODO Auto-generated method stub
				return jedis.hgetAll(hash);
			}
		}));
		
	}
	
	public void put(final String hash, final String key, final String value)
	{
		_client.execute((new JedisOperation()
		{
			
			@Override
			public Object execute(Jedis jedis)
			{
				jedis.hset(hash, key, value);
				if (_timeout > 0)
				{
					jedis.expire(hash, _timeout);
				}
				return null;
			}
		}));
	
	}
	
	
	public void put(final String hash, final Map<String, String> values)
	{
        _client.execute(new JedisOperation()
		{
			
			@Override
			public Object execute(Jedis jedis)
			{
				jedis.hmset(hash, values);
				if (_timeout > 0)
				{
					jedis.expire(hash, _timeout);
				}
				return null;
			}
		});

	}
	
	public void setKey(final String key, final String value) {

        _client.execute(new JedisOperation()
		{
			
			@Override
			public Object execute(Jedis jedis)
			{
				jedis.set(key, value);
				if (_timeout > 0)
				{
					jedis.expire(key, _timeout);
				}
				return null;
			}
		});

	}
	
	public void setNonExpiringKey(final String key, final String value) {
		
		_client.execute(new JedisOperation()
		{
			
			@Override
			public Object execute(Jedis jedis)
			{
				jedis.set(key, value);

				return null;
			}
		});

	}
	
	
	
	public String getKeyValue(final String key) {
		
		return (String)_client.execute(new JedisOperation()
		{
			
			@Override
			public Object execute(Jedis jedis)
			{
				// TODO Auto-generated method stub
				return jedis.get(key);
			}
		});

	}
	
	public void removeKey(final String key) {

        _client.execute(new JedisOperation()
		{
			
			@Override
			public Object execute(Jedis jedis)
			{
				// TODO Auto-generated method stub
				return jedis.del(key);
			}
		});

	}
	
	public void addMemberToSet(final String key, final String member) {

        _client.execute(new JedisOperation()
		{
			
			@Override
			public Object execute(Jedis jedis)
			{
				jedis.sadd(key, member);				
				return null;
			}
		});

	}
	
	public Set<String> getMembersFromSet(final String key) {
		return (Set<String>)_client.execute(new JedisOperation()
		{
			
			@Override
			public Object execute(Jedis jedis)
			{
				return jedis.smembers(key);
			}
		});
	}
	
	public void removeMemberFromSet(final String key, final String member) {
        _client.execute(new JedisOperation()
		{
			
			@Override
			public Object execute(Jedis jedis)
			{
				jedis.srem(key, member);
				return null;
			}
		});

	}
	
	public void delete(final String hash, final String key)
	{
        _client.execute( new JedisOperation()
		{
			
			@Override
			public Object execute(Jedis jedis)
			{
				jedis.hdel(hash, key);
				return null;
			}
		});
	}
	
	public void deleteAll(final String hash)
	{
        _client.execute(new JedisOperation()
		{
			
			@Override
			public Object execute(Jedis jedis)
			{
				jedis.del(hash);
				return null;
			}
		});
	}
	
	
}
