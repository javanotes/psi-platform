package com.reactiveminds.psi.server;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.EntryStore;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.reactiveminds.psi.server.loaders.RawKeyLoadStrategy;
import com.reactiveminds.psi.common.DataWrapper;
import com.reactiveminds.psi.common.err.InternalOperationFailed;
import com.reactiveminds.psi.common.err.ReadThroughOperationException;
import com.reactiveminds.psi.common.err.WriteThroughOperationException;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * The backing map store implementation that will write to Kafka topic and
 * load from a persistent store
 */
class DefaultMapStore implements EntryStore<byte[], DataWrapper>, DiscoverableMapStore, EntryEvictedListener<byte[], DataWrapper> {
    private static final Logger log = LoggerFactory.getLogger(DefaultMapStore.class);
    public DefaultMapStore(Properties p) {
        props = p;
    }
    private Properties props;
    @PostConstruct
    void init(){
        map = props.getProperty("map");
        Assert.notNull(map, "'map' name not set for store config: Internal error");
        topic = props.getProperty("event.topic.name");
        Assert.notNull(topic, "Property not set `event.topic.name`. mapstore: "+map);
        try {
            keyLoadStrategy = beanFactory.getBean(KeyLoadStrategy.class);
            keyLoadStrategy.initialize(props);
        }
        catch (BeansException | IllegalArgumentException e) {
            Assert.notNull(keyLoadStrategy, e.getMessage()+". mapstore: "+map);
        }
        log.info("Configured data map: {}, with stream: {}", map, topic);
    }
    @Autowired
    BeanFactory beanFactory;

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public String getMap() {
        return map;
    }

    private String topic;
    private String map;
    private String strategyBean;

    private KeyLoadStrategy keyLoadStrategy;

    @Autowired
    SpecificAvroDeserializer de;
    void only_for_testing(byte[] k){
        SpecificRecord record = de.deserialize(null, k);
        if(!record.getClass().getSimpleName().endsWith("Key")){
            throw new IllegalArgumentException("<only_for_testing> Unexpected key schema: "+record.getClass());
        }
    }
    private void publish(byte[] key, DataWrapper value) throws ExecutionException, InterruptedException {
        KafkaTemplate<byte[], byte[]> producer = beanFactory.getBean("kafkaTemplate", KafkaTemplate.class);
        producer.send(topic, key, value != null ? value.getPayload() : null).get();
    }
    @Override
    public void store(byte[] bytes, MetadataAwareValue<DataWrapper> metadataAwareValue) {
        try {
            if (!metadataAwareValue.getValue().isTransactional()) {
                only_for_testing(bytes);
                publish(bytes, metadataAwareValue.getValue());

            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        } catch (ExecutionException e) {
            throw new WriteThroughOperationException("Exception on publishing save event", e.getCause());
        }
        catch (Exception e) {
            throw new WriteThroughOperationException("Exception on publishing save event", e);
        }
    }

    @Override
    public void storeAll(Map<byte[], MetadataAwareValue<DataWrapper>> map) {
        map.forEach((b,m) -> store(b,m));
    }

    @Override
    public void delete(byte[] bytes) {
        //only possible through transaction
    }

    @Override
    public void deleteAll(Collection<byte[]> collection) {
        collection.forEach(b -> delete(b));
    }
    @Autowired
    SpecificAvroSerializer serializer;
    @Autowired
    SpecificAvroDeserializer deserializer;

    @Override
    public MetadataAwareValue<DataWrapper> load(byte[] bytes) {
        try
        {
            if(keyLoadStrategy instanceof RawKeyLoadStrategy){
                RawKeyLoadStrategy rawKeyLoadStrategy = (RawKeyLoadStrategy) keyLoadStrategy;
                byte[] value = rawKeyLoadStrategy.findByKey(bytes);
                if(value != null)
                    return new MetadataAwareValue<>(new DataWrapper(value));
            }
            else{
                SpecificRecord key = deserializer.deserialize(topic, bytes);
                SpecificRecord record = keyLoadStrategy.findByKey(key);
                if(record != null)
                    return new MetadataAwareValue<>(new DataWrapper(serializer.serialize(topic, record)));
            }

        }
        catch (Exception e) {
            throw new ReadThroughOperationException("Exception on loading from persistent storage", e);
        }
        return null;
    }

    @Override
    public void entryEvicted(EntryEvent<byte[], DataWrapper> entryEvent) {
        if (entryEvent != null && entryEvent.getKey() != null) {
            try {
                publish(entryEvent.getKey(), entryEvent.getValue());
            } catch (ExecutionException e) {
                throw new InternalOperationFailed("Unable to publish entry on eviction. This may be okay in most cases.", e);
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class KeyAndValue{
        byte[] key;

        public KeyAndValue(byte[] key, MetadataAwareValue<DataWrapper> val) {
            this.key = key;
            this.val = val;
        }

        MetadataAwareValue<DataWrapper> val;
    }
    @Override
    public Map<byte[], MetadataAwareValue<DataWrapper>> loadAll(Collection<byte[]> collection) {
        return collection.parallelStream().map( b -> new KeyAndValue(b, load(b))).collect(Collectors.toMap(k -> k.key, k -> k.val));
    }

    @Override
    public Iterable<byte[]> loadAllKeys() {
        return null;
    }
}
