package com.ndustrialio.cacheutils;


import org.joda.time.DateTime;

public class ValuesCache extends TimeHashingCache
{
    // BECAUSE I WISH IT!
    public static final int REDIS_DB = 10;


	public ValuesCache(RedisClient client)
	{
		super(client);
	}

	private String getKey(int field_id, int window)
	{
		return field_id+":"+window;
	}
	
	public void put(int field_id, int window, DateTime timestamp, String value)
	{
		super.put(getKey(field_id, window), timestamp, value);
	}

	
	public Value get(int field_id, int window, DateTime timestamp)
	{
		return super.get(getKey(field_id, window), timestamp);
	}

	public Value getClosestBefore(int field_id, int window, DateTime timestamp, boolean inclusive)
	{
		return super.getClosestBefore(getKey(field_id, window), timestamp, inclusive);
	}

	public Value getClosestAfter(int field_id, int window, DateTime timestamp, boolean inclusive)
	{
		return super.getClosestAfter(getKey(field_id, window), timestamp, inclusive);

	}


}
