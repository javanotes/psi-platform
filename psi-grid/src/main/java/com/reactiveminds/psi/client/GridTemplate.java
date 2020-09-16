package com.reactiveminds.psi.client;

import com.hazelcast.core.HazelcastInstance;
import com.reactiveminds.psi.common.DataWrapper;
import com.reactiveminds.psi.common.err.LockWaitTimeoutException;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

class GridTemplate implements GridOperation {
    private final String mapName;

    @Autowired
    SpecificAvroSerializer serializer;
    @Autowired
    SpecificAvroDeserializer deserializer;
    @Autowired
    HazelcastInstance hazelcast;
    @Value("${psi.grid.client.lockleaseTimeSec:180}")
    private long leaseTimeSec;
    public GridTemplate(String mapName) {
        this.mapName = mapName;
    }

    @Override
    public <K extends SpecificRecord, V extends SpecificRecord> V get(K key) {
        byte[] bytes = serializer.serialize(null, key);
        DataWrapper o = (DataWrapper) hazelcast.getMap(mapName).get(bytes);
        if(o != null && o.getPayload() != null){
            return (V) deserializer.deserialize(null, o.getPayload());
        }
        return null;
    }

    @Override
    public <K extends SpecificRecord, V extends SpecificRecord> void put(K key, V value) {
        byte[] k = serializer.serialize(null, key);
        byte[] v = serializer.serialize(null, value);
        hazelcast.getMap(mapName).set(k,new DataWrapper(v));
    }
    //for testing purpose
    <K extends SpecificRecord> boolean forceUnlock(K key){
        byte[] bytes = serializer.serialize(null, key);
        hazelcast.getMap(mapName).forceUnlock(bytes);
        return true;
    }
    @Override
    public <K extends SpecificRecord, V extends SpecificRecord> V lockAndGet(K key, long lockWait, TimeUnit unit) {
        byte[] k = serializer.serialize(null, key);
        try {
            boolean lock = hazelcast.getMap(mapName).tryLock(k, lockWait, unit, leaseTimeSec, TimeUnit.SECONDS);
            if(!lock)
                throw new LockWaitTimeoutException(key.toString());
            DataWrapper o = (DataWrapper) hazelcast.getMap(mapName).get(k);
            if(o != null && o.getPayload() != null){
                return (V) deserializer.deserialize(null, o.getPayload());
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return null;
    }

    @Override
    public <K extends SpecificRecord, V extends SpecificRecord> void putAndUnlock(K key, V value) {
        byte[] k = null;
        try {
            k = serializer.serialize(null, key);
            byte[] v = serializer.serialize(null, value);
            hazelcast.getMap(mapName).set(k,new DataWrapper(v));
        }
        finally {
            if(k!= null)
                hazelcast.getMap(mapName).unlock(k);
        }

    }

    @Override
    public <K extends SpecificRecord> void evict(K... keyList) {
        Arrays.stream(keyList).map(key -> serializer.serialize(null, key))
                .forEach(k -> hazelcast.getMap(mapName).evict(k));
    }

    @Override
    public <K extends SpecificRecord> void evictAll() {
        hazelcast.getMap(mapName).evictAll();
    }

    @Override
    public <K extends SpecificRecord, V extends SpecificRecord, O extends SpecificRecord> boolean compareAndPut(K key, V value, O oldValue) {
        byte[] k = serializer.serialize(null, key);
        byte[] v = serializer.serialize(null, value);
        byte[] o = serializer.serialize(null, oldValue);
        return hazelcast.getMap(mapName).replace(k,new DataWrapper(o),new DataWrapper(v));
    }


}
