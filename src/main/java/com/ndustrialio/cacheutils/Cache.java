package com.ndustrialio.cacheutils;

import java.util.Map;

/**
 * Created by jmhunt on 12/31/15.
 */
public interface Cache<T>
{
    /**
     * Gets a single field from a hash
     * @param hash
     * @param field
     * @return cached value, or null if none
     */
    T get(final String hash, final String field);

    /**
     * Gets all fields from a hash
     * @param hash Has
     * @return All cached values, or an empty Map if none
     */
    Map<String, T> getAll(final String hash);

    /**
     * Cache a single value
     * @param hash
     * @param field
     * @param value
     */
    void put(final String hash, final String field, final T value);

    /**
     * Cache an entire hash of values
     * @param hash
     * @param values
     */
    void put(final String hash, final Map<String, T> values);

    /**
     * Delete a specific field of a hash
     * @param hash
     * @param field
     */
    void delete(final String hash, final String field);

    /**
     * Delete an entire hash
     * @param hash
     */
    void deleteAll(final String hash);

}
