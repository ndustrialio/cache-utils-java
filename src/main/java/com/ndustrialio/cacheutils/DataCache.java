package com.ndustrialio.cacheutils;

import org.joda.time.DateTime;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jmhunt on 3/31/17.
 */
public class DataCache<T> implements ITimeSeriesCache<T>
{

    // A TTLNode is kept in a TreeMap according to its last access time.
    // Node: this is basically a fancy tuple.. due to the generics this has to be
    // static, otherwise it becomes DataCache<T>.TTLNode, which doesn't work with instanceof
    // in the equals() implementation
    static class TTLNode
    {
        public DateTime timestamp;

        public String key;


        public TTLNode(DateTime timestamp, String key)
        {
            this.timestamp = timestamp;
            this.key = key;
        }

        @Override
        public boolean equals(Object o)
        {
            if (o instanceof TTLNode)
            {
                TTLNode t = (TTLNode)o;

                return this.timestamp.isEqual(t.timestamp)
                        && this.key.equals(t.key);
            } else
            {
                return false;
            }
        }

        @Override
        public int hashCode()
        {
            int result = timestamp.hashCode();
            result = 31 * result + key.hashCode();
            return result;
        }
    }

    // A cache node holds a value and its last access time
    private class CacheNode
    {
        DateTime accessTime;

        public T value;

        CacheNode(T value, DateTime accessTime)
        {
            this.accessTime = accessTime;
            this.value = value;
        }
    }


    // 5 hours default
    public static final int DEFAULT_TTL = 60*60*5;

    // Cache structure
    private Map<String, TreeMap<DateTime, CacheNode>> _data;

    // TTL structure
    // TODO: make this optional, currently is not
    private TreeMap<DateTime, Set<TTLNode>> _ttlNodes;

    private int _ttl;

    public DataCache(int ttl)
    {
        _data = new HashMap<>();

        _ttlNodes = new TreeMap<>();

        _ttl = ttl;
    }

    public DataCache()
    {
        this(DEFAULT_TTL);
    }


    @Override
    public Value<T> get(String key, DateTime timestamp)
    {
        // Do filter on key
        TreeMap<DateTime, CacheNode> dataByKey = _data.get(key);

        if ((dataByKey == null) || dataByKey.isEmpty())
        {
            return null;
        }


        CacheNode n = dataByKey.get(timestamp);

        if (n != null)
        {
            // Update TTL map.. remove and reinsert with new access time
            // Make sure to store new access time
            n.accessTime = updateTTL(n.accessTime, new TTLNode(timestamp, key));

            return new Value<T>(timestamp, n.value);

        } else
        {
            return null;
        }
    }


    @Override
    public List<Value<T>> get(DateTime timestamp, String... keys)
    {
        List<Optional<CacheNode>> cacheNodes = Arrays.stream(keys)
                .map(key -> Optional.ofNullable(_data.get(key))) // Look up in data map
                .map(data -> Optional.ofNullable(data.map(d -> d.isEmpty() ? null : d).orElse(null))) // Handle case in which map is empty (treat as null)
                .map(data ->  Optional.ofNullable(data.map(d->d.get(timestamp)).orElse(null))) // Extract CacheNode by timestamp
                .collect(Collectors.toList()); // Collect to list


        List<Value<T>> ret = new ArrayList<>();

        // Update access time and return list.. couldn't find a way to do this in streams
        for(int i = 0; i < keys.length; i++) {
            String key = keys[i];
            Optional<CacheNode> o = cacheNodes.get(i);

            if (o.isPresent()) {

                CacheNode node = o.get();

                node.accessTime = updateTTL(node.accessTime, new TTLNode(timestamp, key));

                ret.add(new Value<T>(timestamp, node.value));
            } else{
                ret.add(null);
            }
        }

        return ret;
    }
    /**
     * Updates the TTL map.  Removes the provided TTLNode, if
     * extant, and then inserts a new one at the current time
     * @param accessTime The previous access time of this TTL node, or null if none
     * @param ttlNode TTLNode to remove/reinsert
     * @return new access time
     */
    private DateTime updateTTL(DateTime accessTime, TTLNode ttlNode)
    {
        Set<TTLNode> ttlNodes;

        // Remove this TTLNode from the set of TTLNodes expiring
        // at this accessTime. If accessTime is null, don't
        // need to remove anything.
        if (accessTime != null)
        {
            ttlNodes = _ttlNodes.get(accessTime);

            if (ttlNodes != null)
            {
                ttlNodes.remove(ttlNode);

                // Possibly the last node at this time.  Clean up
                if (ttlNodes.isEmpty()) _ttlNodes.remove(accessTime);
            }
        }

        // Now putting new access time in TTL map
        DateTime newAccessTime = DateTime.now();

        // Anybody else?
        ttlNodes = _ttlNodes.get(newAccessTime);

        if (ttlNodes == null)
        {
            // Create and put set at new access time
            ttlNodes = new HashSet<>();
            _ttlNodes.put(newAccessTime, ttlNodes);
        }

        // Add ttlNode
        ttlNodes.add(ttlNode);

        return newAccessTime;
    }

    @Override
    public void put(String key, DateTime timestamp, T value)
    {
        TreeMap<DateTime, CacheNode> dataByKey = _data.get(key);

        if (dataByKey == null)
        {
            dataByKey = new TreeMap<>();
            _data.put(key, dataByKey);
        }


        // Check if something is already here, and if so, remove it from the TTL map!
        CacheNode replacedNode = dataByKey.remove(timestamp);

        DateTime putTime;
        if (replacedNode != null)
        {
            // extant in TTL map, do an update
            putTime = updateTTL(replacedNode.accessTime, new TTLNode(timestamp, key));
        } else
        {
            // Not extant in cache, just add a new node.
            putTime = updateTTL(null, new TTLNode(timestamp, key));
        }

        // Insert new CacheNode
        dataByKey.put(timestamp, new CacheNode(value, putTime));

        // Cleanup a portion of cache memory
        cleanup(50);

    }


    public void cleanup(int entries)
    {
        // First, compute the time to live threshold
        DateTime ttlThresh = DateTime.now().minusSeconds(_ttl);

        // Anything older than this will be evicted.
        SortedMap<DateTime, Set<TTLNode>> evictedMap = _ttlNodes.headMap(ttlThresh);

        int i = 0;

        Iterator<Map.Entry<DateTime, Set<TTLNode>>> iter = evictedMap.entrySet().iterator();

        while(iter.hasNext() && i < entries)
        {
            Map.Entry<DateTime, Set<TTLNode>> e = iter.next();

            // Remove all CacheNodes in the cache expiring at this time
            for (TTLNode n : e.getValue())
            {
                // Remove CacheNode from data map
                TreeMap<DateTime, CacheNode> m = _data.get(n.key);
                m.remove(n.timestamp);
                // Don't allow empty danging maps
                if (m.isEmpty()) _data.remove(n.key);
            }

            // Clean up TTL map
            iter.remove();

            i++;
        }


    }

    @Override
    public Value<T> getClosestAfter(String key, DateTime timestamp, boolean inclusive)
    {

        // Do filter on key
        TreeMap<DateTime, CacheNode> dataByKey = _data.get(key);

        if ((dataByKey == null) || dataByKey.isEmpty())
        {
            return null;
        }

        Map.Entry<DateTime, CacheNode> e;


        if (inclusive)
        {
            // ceilingEntry() includes the specified key
            e = dataByKey.ceilingEntry(timestamp);
        } else
        {
            // higherEntry() does not
            e = dataByKey.higherEntry(timestamp);
        }

        if (e != null)
        {
            // Update TTL map.. remove and reinsert with new access time
            // Make sure to store new access time
            e.getValue().accessTime = updateTTL(e.getValue().accessTime, new TTLNode(e.getKey(), key));



            return new Value<T>(e.getKey(), e.getValue().value);
        } else
        {
            return null;
        }
    }

    @Override
    public Value<T> getClosestBefore(String key, DateTime timestamp, boolean inclusive)
    {
        // Do filter on key
        TreeMap<DateTime, CacheNode> dataByKey = _data.get(key);

        if ((dataByKey == null) || dataByKey.isEmpty())
        {
            return null;
        }


        Map.Entry<DateTime, CacheNode> e;


        if (inclusive)
        {
            // floorEntry() includes the specified key
            e = dataByKey.floorEntry(timestamp);
        } else
        {
            // lowerEntry() does not
            e = dataByKey.lowerEntry(timestamp);
        }

        if (e != null)
        {
            // Update TTL map.. remove and reinsert with new access time
            // Make sure to store new access time
            e.getValue().accessTime =  updateTTL(e.getValue().accessTime, new TTLNode(e.getKey(), key));


            return new Value<T>(e.getKey(), e.getValue().value);
        } else
        {
            return null;
        }
    }
}
