package com.ndustrialio.cacheutils;

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jmhunt on 7/12/16.
 */
public class TimedEvictionPolicy extends BaseEvictionPolicy
{


    private static class EvictionEntry
    {
        public String key, hash;
        public long insertedAt;
        public EvictionEntry(String key, String hash, long insertedAt)
        {
            this.key = key;
            this.hash = hash;
            this.insertedAt = insertedAt;
        }
    }

    private EvictionListener _listener;

    private List<EvictionEntry> _data;

    private long _timeout;

    public TimedEvictionPolicy(long timeoutMS)
    {
        _timeout = timeoutMS;
        _data = new ArrayList<>();
    }

    @Override
    public void add(String key, String hash)
    {
        // Add this entry to the end of list
        // Save the time it was inserted
        _data.add(new EvictionEntry(key, hash, DateTime.now().getMillis()));

        Iterator<EvictionEntry> iterator = _data.iterator();

        // Clean up all the entries that have expired
        while(iterator.hasNext())
        {
            EvictionEntry e = iterator.next();

            if ((DateTime.now().getMillis() - e.insertedAt) >= _timeout)
            {
                iterator.remove();

                // Notify cache that element was evicted
                _listener.elementEvicted(e.key, e.hash);
            } else
            {
                // Since they're in order, we can stop at the first one that
                // is not too old
                break;
            }
        }

    }

    @Override
    public void get(String key, String hash)
    {
        // Don't do shit
    }

    @Override
    public void del(String key, String hash)
    {
        // Delete is very expensive..
        Iterator<EvictionEntry> iterator = _data.iterator();

        while(iterator.hasNext())
        {
            EvictionEntry e = iterator.next();

            if (e.key.equals(key) && e.hash.equals(hash))
            {
                // Should be only one
                iterator.remove();
                break;
            }
        }
    }


    @Override
    public void setEvictionListener(EvictionListener e)
    {
        _listener = e;
    }
}
