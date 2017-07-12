package com.ndustrialio.cacheutils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;


/**
 * Created by jmhunt on 2/22/16.
 */
public class LRUEvictionPolicy extends BaseEvictionPolicy
{

    private EvictionListener _listener;

    private LRUMap _map;


    private interface EldestEntryListener
    {
        void eldestRemoved(Map.Entry<String, String> eldest);
    }

    private class LRUMap extends LinkedHashMap<String, String>
    {
        private int _capacity;

        private EldestEntryListener _listener;

        public LRUMap(int capacity,  EldestEntryListener e)
        {
            super(capacity + 1, 1.1f, true);

            _capacity = capacity;

            _listener = e;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest)
        {
            if (size() > _capacity)
            {
                _listener.eldestRemoved(eldest);

                return true;
            } else
            {
                return false;
            }
        }
    }

    public LRUEvictionPolicy(int capacity)
    {
        _map = new LRUMap(capacity, new EldestEntryListener()
        {
            @Override
            public void eldestRemoved(Map.Entry<String, String> eldest)
            {
                String[] splitKey = eldest.getKey().split(Pattern.quote("+"));
                _listener.elementEvicted(splitKey[0], splitKey[1]);
            }
        });

    }


    @Override
    public void add(String key, String hash)
    {
        _map.put(key+"+"+hash, "");
    }

    @Override
    public void get(String key, String hash)
    {
        //_map.get(hash);
    }

    @Override
    public void del(String key, String hash)
    {
        _map.remove(key+"+"+hash);
    }


    @Override
    public void setEvictionListener(EvictionListener e)
    {
        _listener = e;
    }


}
