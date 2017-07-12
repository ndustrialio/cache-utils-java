package com.ndustrialio.cacheutils;

/**
 * Created by jmhunt on 2/22/16.
 */
public interface IEvictionPolicy
{

    void add(String hash, String field);

    void get(String hash, String field);

    void del(String hash, String field);

    void setEvictionListener(EvictionListener e);
}
