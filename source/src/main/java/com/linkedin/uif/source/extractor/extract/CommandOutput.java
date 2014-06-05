package com.linkedin.uif.source.extractor.extract;

import java.util.Map;

/**
 * Stores the output of a Command into a Map object
 * with types K and V
 * @author stakiar
 *
 * @param <K> the key type of the Map
 * @param <V> the value type of the Map
 */
public interface CommandOutput<K extends Command, V>
{
    public void storeResults(Map<K, V> results);
    public Map<K, V> getResults();
    public void put(K key, V value);
}
