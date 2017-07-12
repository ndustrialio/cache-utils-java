package com.ndustrialio.cacheutils.test;


import com.ndustrialio.cacheutils.ITimeSeriesCache;
import com.ndustrialio.cacheutils.RedisClient;
import com.ndustrialio.cacheutils.TimeScoreValuesCache;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jmhunt on 3/14/17.
 */
public class TimeScoreValuesCacheTest
{
    public static void main(String[] args)
    {
        Map<String, Object> conf = new HashMap<>();

        conf.put("redis_host", "dev");

        TimeScoreValuesCache cache = new TimeScoreValuesCache(new RedisClient(conf, 4));


        DateTime baseTime = DateTime.now().withMillisOfSecond(0).withSecondOfMinute(0);

        DateTime targetTime = baseTime.plusMinutes(10);


        for(int i = 0; i < 100; i++)
        {
            cache.put(100, 0, baseTime, i+"");
            baseTime = baseTime.plusMinutes(1);
        }


        ITimeSeriesCache.Value<String> v = cache.get(100, 0, targetTime);

        System.out.println("Got exactly: " + targetTime + ",  value: " + v.value +", time: " + v.timestamp);


        v = cache.getClosestAfter(100, 0, targetTime);


        System.out.println("Closest after " + targetTime + ", inclusive: value: " + v.value +", time: " + v.timestamp);

        // Try an exclusive get
        v = cache.getClosestAfter(100, 0, targetTime, false);

        System.out.println("Closest after " + targetTime + ", exclusive: value: " + v.value +", time: " + v.timestamp);


        v = cache.getClosestBefore(100, 0, targetTime);

        System.out.println("Closest before " + targetTime + ", inclusive: value: " + v.value +", time: " + v.timestamp);

        // Try an exclusive get
        v = cache.getClosestBefore(100, 0, targetTime, false);

        System.out.println("Closest before " + targetTime + ", exclusive: value: " + v.value +", time: " + v.timestamp);

        // Try a replacement:
        cache.put(100, 0, targetTime, "4500");

        v = cache.get(100, 0, targetTime);

        System.out.println("After replacement: value: " + v.value +", time: " + v.timestamp);


    }
}
