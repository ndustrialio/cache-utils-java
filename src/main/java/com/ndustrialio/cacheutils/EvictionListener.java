package com.ndustrialio.cacheutils;

/**
 * Created by jmhunt on 2/22/16.
 */
public interface EvictionListener
{
    void elementEvicted(String key, String hash);
}
