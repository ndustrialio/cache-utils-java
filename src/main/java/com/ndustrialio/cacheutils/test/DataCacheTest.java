package com.ndustrialio.cacheutils.test;

import com.ndustrialio.cacheutils.DataCache;
import com.ndustrialio.cacheutils.ITimeSeriesCache.Value;
import org.joda.time.DateTime;

/**
 * Created by jmhunt on 3/14/17.
 */
public class DataCacheTest
{
    public static void main(String[] args)
    {
        DataCache<Integer> cache = new DataCache<>(10); // 10 second ttl

        DateTime baseTime = DateTime.now().withSecondOfMinute(0).withMillisOfSecond(0);

        for(int i = 0; i < 5; i++)
        {
            cache.put("test", baseTime, i);
            baseTime = baseTime.plusMinutes(1);
        }


        DateTime targetTime = DateTime.now().plusMinutes(2).withSecondOfMinute(0).withMillisOfSecond(0);

        Value<Integer> v = cache.get("test", targetTime);

        System.out.println("Got exactly: " + targetTime + ",  value: " + v.value +", time: " + v.timestamp);


        v = cache.getClosestAfter("test", targetTime, true);


        System.out.println("Closest after " + targetTime + ", inclusive: value: " + v.value +", time: " + v.timestamp);

        // Try an exclusive get
        v = cache.getClosestAfter("test", targetTime, false);

        System.out.println("Closest after " + targetTime + ", exclusive: value: " + v.value +", time: " + v.timestamp);


        v = cache.getClosestBefore("test", targetTime, true);

        System.out.println("Closest before " + targetTime + ", inclusive: value: " + v.value +", time: " + v.timestamp);

        // Try an exclusive get
        v = cache.getClosestBefore("test", targetTime, false);

        System.out.println("Closest before " + targetTime + ", exclusive: value: " + v.value +", time: " + v.timestamp);

        cache.cleanup(10);



    }
}
