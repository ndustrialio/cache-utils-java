package com.ndustrialio.cacheutils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by jmhunt on 1/1/16.
 */
public class LocalCache<T> implements Cache<T>
{
    // This data structure will emulate the two-tiered hashing of redis
    protected Map<String, Map<String, T>> _data;

    protected IEvictionPolicy _evictionPolicy;

    public LocalCache()
    {
        _data = new HashMap<>();

        // Blank eviction policy
        _evictionPolicy = new IEvictionPolicy()
        {
            @Override
            public void add(String hash, String field)
            {

            }

            @Override
            public void get(String hash, String field)
            {

            }

            @Override
            public void del(String hash, String field)
            {

            }

            @Override
            public void setEvictionListener(EvictionListener e)
            {

            }
        };


    }

    public LocalCache(IEvictionPolicy e)
    {
        _data = new HashMap<String, Map<String, T>>();

        _evictionPolicy = e;


        _evictionPolicy.setEvictionListener(new EvictionListener()
        {
            @Override
            public void elementEvicted(String hash, String field)
            {
                // Break hash apart so we can properly remove it
                //String[] splitKey = hash.split(Pattern.quote("+"));

                Map<String, T> fields = _data.get(hash);

                if (fields != null)
                {
                    fields.remove(field);
                }
            }
        });


    }

    private String concatenate(String hash, String field)
    {
        return hash + "+" + field;
    }


    @Override
    public T get(String hash, String field)
    {
        T ret = null;

        Map<String, T> fields = _data.get(hash);

        if (fields != null)
        {
            ret = fields.get(field);

            // Poke the EvictionPolicy in case it is access-dependent
            _evictionPolicy.get(hash, field);

        }

        return ret;
    }


    @Override
    public Map<String, T> getAll(String hash)
    {
        Map<String, T> ret = new HashMap<String, T>();

        Map<String, T> h = _data.get(hash);

        if (h != null)
        {
            for(Entry<String, T> e : h.entrySet())
            {
                ret.put(e.getKey(), e.getValue());

                // Poke the EvictionPolicy in case it is access-dependent
                _evictionPolicy.get(hash, e.getKey());
            }

        }

        return ret;
    }

    @Override
    public void put(String hash, String field, T value)
    {
        // Store in cache
        Map<String, T> fields = _data.get(hash);

        if (fields == null)
        {
            fields = new HashMap<String, T>();

            _data.put(hash, fields);
        } else
        {
            // A new set of fields wont have a dirty bit
            // so its safe to only clear it here
            fields.remove("dirty_bit");
        }

        fields.put(field, value);

        // Notify eviction policy of new key
        _evictionPolicy.add(hash, field);

    }

    @Override
    public void put(String hash, Map<String, T> values)
    {
        Map<String, T> fields = _data.get(hash);

        if (fields == null)
        {
            fields = new HashMap<String, T>();

            _data.put(hash, fields);
        }

        for(Entry<String, T> e : values.entrySet())
        {
            // Store in cache
            fields.put(e.getKey(), e.getValue());

            // Notify eviction policy of new key
            _evictionPolicy.add(hash, e.getKey());
        }
    }


    @Override
    public void delete(String hash, String field)
    {

        Map<String, T> fields = _data.get(hash);

        if (fields != null)
        {
            fields.remove(field);

            if (fields.isEmpty())
            {
                _data.remove(hash);
            }

            // Notify eviction policy of removal
            _evictionPolicy.del(hash, field);
        }
    }

    @Override
    public void deleteAll(String hash)
    {
        Map<String, T> fields = _data.get(hash);

        if (fields != null)
        {
            for(Entry<String, T> e : fields.entrySet())
            {
                fields.remove(e.getKey());

                // Notify eviction policy of removal
                _evictionPolicy.del(hash, e.getKey());
            }

            _data.remove(hash);
        }
    }
}
