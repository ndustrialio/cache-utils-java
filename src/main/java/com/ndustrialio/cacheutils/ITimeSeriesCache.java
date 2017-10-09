package com.ndustrialio.cacheutils;

import org.joda.time.DateTime;
import org.json.JSONObject;

import java.util.List;

/**
 * Created by jmhunt on 4/4/17.
 */
public interface ITimeSeriesCache<T>
{

    /**
     * Get a value at a timestamp, or null if none
     * @param key Cache key
     * @param timestamp Cache timestamp
     * @return the Value, or null if none.
     */
     Value<T> get(String key, DateTime timestamp);

    /**
     * Get multiple values at a timestamp, or null if none
     * Will always return a list equal to the length of the
     * keys parameter
     * @param timestamp timestamp to search for
     * @param keys keys to search for
     * @return List of values, null if none
     */
    List<Value<T>> get(DateTime timestamp, String... keys);

    /**
     * Put a value in the cachce
     * @param key Cache key
     * @param timestamp Cache timestamp
     * @param value Value to put
     */
     void put(String key, DateTime timestamp, T value);


    /**
     * Get closest value after the provided timestamp
     * @param key Cache key
     * @param timestamp Cache timestamp
     * @param inclusive if true, include timestamp in the results
     * @return
     */
    Value<T> getClosestAfter(String key, DateTime timestamp, boolean inclusive);


    /**
     * Get closest value before the provided timestamp
     * @param key Cache key
     * @param timestamp Cache timestamp
     * @param inclusive if true, include timestamp in the results
     * @return
     */
    Value<T> getClosestBefore(String key, DateTime timestamp, boolean inclusive);

}
