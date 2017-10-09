package com.ndustrialio.cacheutils;

import org.joda.time.DateTime;
import org.json.JSONObject;

/**
 * Created by jmhunt on 10/6/17.
 */
public class Value<T>
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


    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Value<?> value1 = (Value<?>) o;

        if (timestamp != null ? !timestamp.equals(value1.timestamp) : value1.timestamp != null) return false;
        return value != null ? value.equals(value1.value) : value1.value == null;
    }
}
