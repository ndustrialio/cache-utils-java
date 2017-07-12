package com.ndustrialio.cacheutils;

import java.util.regex.Pattern;

/**
 * Created by jmhunt on 7/19/16.
 */
public abstract class BaseEvictionPolicy implements IEvictionPolicy
{


    protected String concatenate(String key, String hash)
    {
        return key + "+" + hash;
    }

    protected String[] split(String combinedKey)
    {
        return combinedKey.split(Pattern.quote("+"));
    }

}
