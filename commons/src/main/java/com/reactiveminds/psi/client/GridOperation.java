package com.reactiveminds.psi.client;

import org.apache.avro.specific.SpecificRecord;

import java.util.concurrent.TimeUnit;

/**
 * No delete
 */
public interface GridOperation {
    /**
     *
     * @param key
     * @param <K>
     * @param <V>
     * @return
     */
    <K extends SpecificRecord, V extends SpecificRecord> V get(K key);

    /**
     *
     * @param key
     * @param value
     * @param <K>
     * @param <V>
     * @return
     */
    <K extends SpecificRecord, V extends SpecificRecord> void put(K key, V value);

    /**
     * Exclusive get. Locks (pessimistic) the given key for certain period of time (`lease time` - configurable at application level)
     * @param key
     * @param lockWait
     * @param unit
     * @param <K>
     * @param <V>
     * @return
     */
    <K extends SpecificRecord, V extends SpecificRecord> V lockAndGet(K key, long lockWait, TimeUnit unit);

    /**
     * Put after the exclusive get
     * @param key
     * @param value
     * @param <K>
     * @param <V>
     * @return
     */
    <K extends SpecificRecord, V extends SpecificRecord> void putAndUnlock(K key, V value);

    /**
     * Evict the given keyset from grid map. Does not change underlying store
     * @param keyList
     * @param <K>
     */
    <K extends SpecificRecord> void evict(K... keyList);

    /**
     * Evict all keys from grid map. Does not change underlying store
     * @param <K>
     */
    <K extends SpecificRecord> void evictAll();
    /**
     * Optimistic locking
     * @param key
     * @param value
     * @param oldValue
     * @param <K>
     * @param <V>
     * @param <O>
     * @return
     */
    <K extends SpecificRecord, V extends SpecificRecord, O extends SpecificRecord> boolean compareAndPut(K key, V value, O oldValue);


}
