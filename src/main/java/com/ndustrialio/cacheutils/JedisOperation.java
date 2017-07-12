package com.ndustrialio.cacheutils;

import redis.clients.jedis.Jedis;

public interface JedisOperation
{	
	Object execute(Jedis jedis);
}
