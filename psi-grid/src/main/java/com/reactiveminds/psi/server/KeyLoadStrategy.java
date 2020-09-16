package com.reactiveminds.psi.server;

import org.apache.avro.specific.SpecificRecord;
import org.springframework.beans.factory.BeanInitializationException;

import java.io.IOException;
import java.util.Properties;

public interface KeyLoadStrategy<K extends SpecificRecord,V extends SpecificRecord> {
    /**
     * Load a given key value from backing store
     * @param key
     * @return
     */
     V findByKey(K key) throws Exception;
     void initialize(Properties p) throws BeanInitializationException;
}
