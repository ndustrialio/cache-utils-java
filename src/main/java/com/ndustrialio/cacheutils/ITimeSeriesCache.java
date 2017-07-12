package com.ndustrialio.cacheutils;

import org.joda.time.DateTime;
import org.json.JSONObject;

/**
 * Created by jmhunt on 4/4/17.
 */
public interface ITimeSeriesCache<T>
{
    class Value<T>
    {
        public DateTime timestamp;

        public T value;

        public Value(DateTime timestamp, T value)
        {
            this.timestamp = timestamp;
            this.value = value;
        }

        @Override
        public String toString()
        {
            JSONObject obj = new JSONObject();

            obj.put("t", timestamp.getMillis());
            obj.put("v", value);


            return obj.toString();
        }

    }

    /**
     * Get a value at a timestamp, or null if none
     * @param key Cache key
     * @param timestamp Cache timestamp
     * @return the Value, or null if none.
     */
     Value<T> get(String key, DateTime timestamp);

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
