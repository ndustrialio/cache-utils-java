package com.ndustrialio.cacheutils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jmhunt on 12/22/16.
 */
public class EvictionTrackingCache<T> extends LocalCache<T>
{

    public interface DataEvictedListener
    {
        void dataEvicted(String hash);
    }

    protected DataEvictedListener _listener = null;

    protected Map<String, Map<String, String>> _evictedBits;

    public EvictionTrackingCache(IEvictionPolicy policy)
    {
        super(policy);

        _evictedBits = new HashMap<>();

        _evictionPolicy.setEvictionListener(new EvictionListener()
        {
            @Override
            public void elementEvicted(String hash, String field)
            {

                Map<String, T> fields = _data.get(hash);

                if (fields != null)
                {
                    fields.remove(field);

                    if (fields.isEmpty())
                    {
                        // Just removed the last field in this hash.. remove
                        // the hash all together and no longer mark it as evicted
                        _data.remove(hash);

                        _evictedBits.remove(hash);

                    } else
                    {
                        // Add this field to the map of evicted fields
                        Map<String, String> evictedFields = _evictedBits.get(hash);

                        if (evictedFields == null)
                        {
                            evictedFields = new HashMap<>();

                            _evictedBits.put(hash, evictedFields);
                        }

                        // Doesn't need to hold a value
                        evictedFields.put(field, "");
                    }

                }
            }
        });
    }

    public void setDataEvictedListener(DataEvictedListener listener)
    {
        _listener = listener;
    }


    @Override
    public Map<String, T> getAll(String hash)
    {
        Map<String, T> ret = super.getAll(hash);

        if (_evictedBits.containsKey(hash))
        {
            // Notify caller of eviction in this hash
            if (_listener != null)
            {
                _listener.dataEvicted(hash);
            }
        }

        return ret;

    }

    @Override
    public void put(String hash, String field, T value)
    {
        super.put(hash, field, value);

        Map<String, String> evictedFields = _evictedBits.get(hash);

        if (evictedFields != null)
        {
            evictedFields.remove(field);

            if (evictedFields.isEmpty())
            {
                _evictedBits.remove(hash);
            }
        }

    }


    @Override
    public void put(String hash, Map<String, T> values)
    {
        super.put(hash, values);

        // Entire hash was put, so it can't be considered dirty
        _evictedBits.remove(hash);
    }

    @Override
    public void delete(String hash, String field)
    {
        super.delete(hash, field);

        Map<String, String> evictedFields = _evictedBits.get(hash);

        if (evictedFields != null)
        {
            evictedFields.remove(field);

            if (evictedFields.isEmpty())
            {
                _evictedBits.remove(hash);
            }
        }
    }

    @Override
    public void deleteAll(String hash)
    {
        super.deleteAll(hash);

        // Entire hash was deleted, so it can't be considered dirty
        _evictedBits.remove(hash);
    }

}
