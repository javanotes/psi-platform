package com.reactiveminds.psi.client;

import org.apache.avro.specific.SpecificRecord;

public interface GridTransactionContext {
    /**
     * Start a transaction
     */
    void begin();

    /**
     * Commit transaction
     */
    void commit();

    /**
     * Rollback transaction
     */
    void rollback();
    /**
     * @param key
     * @param map
     * @param <K>
     * @param <V>
     * @return
     */
    <K extends SpecificRecord, V extends SpecificRecord> V get(K key, String map);

    /**
     *
     * @param key
     * @param map
     * @param <K>
     * @return
     */
    <K extends SpecificRecord> boolean delete(K key, String map);
    /**
     *
     * @param key
     * @param map
     * @param <K>
     * @param <V>
     * @return
     */
    <K extends SpecificRecord, V extends SpecificRecord> V getForUpdate(K key, String map);

    /**
     * save
     * @param key
     * @param value
     * @param map
     * @param <K>
     * @param <V>
     */
    <K extends SpecificRecord, V extends SpecificRecord> void put(K key, V value, String map);
}
