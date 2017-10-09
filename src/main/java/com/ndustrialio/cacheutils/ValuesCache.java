package com.ndustrialio.cacheutils;


import org.joda.time.DateTime;
import java.util.Arrays;
import java.util.List;

public class ValuesCache
{
    // BECAUSE I WISH IT!
    public static final int REDIS_DB = 10;

    private ITimeSeriesCache<String> _cache;


	public ValuesCache(ITimeSeriesCache<String> cache){
		_cache = cache;
	}

	private static String getKey(String field_id, int window)
	{
		return field_id+":"+window;
	}
	
	public void put(String field_id, int window, DateTime timestamp, String value)
	{
		_cache.put(getKey(field_id, window), timestamp, value);
	}

	
	public Value<String> get(String field_id, int window, DateTime timestamp)
	{
		return _cache.get(getKey(field_id, window), timestamp);
	}

	public Value<String> getClosestBefore(String field_id, int window, DateTime timestamp, boolean inclusive)
	{
		return _cache.getClosestBefore(getKey(field_id, window), timestamp, inclusive);
	}

	public Value<String> getClosestAfter(String field_id, int window, DateTime timestamp, boolean inclusive)
	{
		return _cache.getClosestAfter(getKey(field_id, window), timestamp, inclusive);

	}

	public List<Value<String>> get(DateTime timestamp, int window, String... field_ids)
	{
		return _cache.get(timestamp, Arrays.stream(field_ids).map(field_id -> getKey(field_id, window)).toArray(String[]::new));
	}


}
