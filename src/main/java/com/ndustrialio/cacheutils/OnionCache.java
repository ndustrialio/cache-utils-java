package com.ndustrialio.cacheutils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jmhunt on 2/7/17.
 */
public class OnionCache<T> implements Cache<T>
{
    List<Cache<T>> _layers;

    public OnionCache(Cache<T>... layers)
    {
        _layers = Arrays.asList(layers);

    }


    @Override
    public T get(String hash, String field)
    {
        for (Cache<T> layer : _layers)
        {
            T value = layer.get(hash, field);

            if (value != null)
            {
                return value;
            }
        }

        return null;
    }

    @Override
    public Map<String, T> getAll(String hash)
    {

        for (Cache<T> layer : _layers)
        {
            Map<String, T> value = layer.getAll(hash);

            if (!value.isEmpty())
            {
                return value;
            }
        }

        return new HashMap<>();
    }

    @Override
    public void put(String hash, String field, T value)
    {
        for (Cache<T> layer : _layers)
        {
            layer.put(hash, field, value);
        }
    }

    @Override
    public void put(String hash, Map<String, T> values)
    {
        for (Cache<T> layer : _layers)
        {
            layer.put(hash, values);
        }
    }

    @Override
    public void delete(String hash, String field)
    {
        for (Cache<T> layer : _layers)
        {
            layer.delete(hash, field);
        }
    }

    @Override
    public void deleteAll(String hash)
    {
        for (Cache<T> layer : _layers)
        {
            layer.deleteAll(hash);
        }
    }
}
