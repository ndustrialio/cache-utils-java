package com.ndustrialio.cacheutils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

/**
 * Created by jmhunt on 2/23/16.
 */
public class RandomEvictionPolicy extends BaseEvictionPolicy
{
    private int _capacity, _maxItemsToDelete;

    private HashMap<String, String> _data;

    private EvictionListener _listener;

    private Random _rand;



    public RandomEvictionPolicy(int capacity)
    {
        _data = new HashMap<String, String>();

        _rand = new Random();

        _capacity = capacity;

        // 10% of capacity, with a ceiling of 300...
        _maxItemsToDelete = (int)(0.1f * (float)_capacity);
        _maxItemsToDelete = (_maxItemsToDelete <= 300) ? _maxItemsToDelete : 300;
    }

    private void deleteItems(int itemsToDelete)
    {
        ArrayList<String> hashes = new ArrayList<String>(_data.keySet());

        while(itemsToDelete > 0)
        {
            int hashIndex = randomIndex(hashes.size());

            String doomedHash = hashes.get(hashIndex);

            _data.remove(doomedHash);

            String[] splitHash = split(doomedHash);
            _listener.elementEvicted(splitHash[0], splitHash[1]);

            itemsToDelete--;

        }

    }

    private void makeRoom(int capacity)
    {

        // The size we would be if we added the new elements
        // in without a deletion
        int provisionalSize = _data.size() + capacity;

        int itemsToDelete = (provisionalSize - _data.size());


        if (itemsToDelete >= _maxItemsToDelete)
        {
            //LOG.info("Deleting " + itemsToDelete + " items");
            deleteItems(itemsToDelete);

        }

    }

    private int randomIndex(int max)
    {
        return _rand.nextInt(max);
    }

    @Override
    public void add(String key, String hash)
    {
        makeRoom(1);

        _data.put(concatenate(key, hash), "");
    }

    @Override
    public void get(String key, String hash)
    {
        // Doesn't change anything with this eviction strategy
    }

    @Override
    public void del(String key, String hash)
    {
        _data.remove(hash);
    }

    @Override
    public void setEvictionListener(EvictionListener e)
    {
        _listener = e;
    }
}
