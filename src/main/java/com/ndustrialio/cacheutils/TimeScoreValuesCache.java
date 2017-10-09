package com.ndustrialio.cacheutils;

import com.ndustrialio.cacheutils.RedisClient;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jmhunt on 3/15/17.
 */
@Deprecated
public class TimeScoreValuesCache extends TimeScoreCache
{
    public TimeScoreValuesCache(RedisClient client)
    {
        super(client);
    }

    public Value<String> getClosestAfter(String field_id, int window, DateTime timestamp)
    {
        return super.getClosestAfter(field_id+":"+window, timestamp, true);
    }

    public Value<String> getClosestAfter(String field_id, int window, DateTime timestamp, boolean inclusive)
    {
        return super.getClosestAfter(field_id+":"+window, timestamp, inclusive);
    }

    public Value<String> getClosestBefore(String field_id, int window, DateTime timestamp)
    {
        return super.getClosestBefore(field_id+":"+window, timestamp, true);
    }

    public Value<String> getClosestBefore(int field_id, int window, DateTime timestamp, boolean inclusive)
    {
        return super.getClosestBefore(field_id+":"+window, timestamp, inclusive);
    }


    public void put(String field_id, int window, DateTime timestamp, String value)
    {
        super.put(field_id+":"+window, timestamp, value);
    }

    public Value<String> get(String field_id, int window, DateTime timestamp)
    {
        return super.get(field_id+":"+window, timestamp);
    }

    public List<Value<String>> get(DateTime timestamp, int window, String... field_ids)
    {
        return super.get(timestamp, Arrays.stream(field_ids).map(field_id-> field_id+":"+window).toArray(String[]::new));
    }

}
