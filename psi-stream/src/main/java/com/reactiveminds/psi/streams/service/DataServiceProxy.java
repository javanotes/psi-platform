package com.reactiveminds.psi.streams.service;

import com.reactiveminds.psi.streams.config.HostStoreInfo;
import com.reactiveminds.psi.streams.config.StreamConfiguration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Component
class DataServiceProxy implements StreamsDataService {

    @Autowired
    StreamConfiguration config;
    @Autowired
    SimpleHttpService edgeService;

    @Override
    public String findByKey(String map, String base64Key){
        byte[] key = Base64.getDecoder().decode(base64Key);
        StreamsMetadata metadata = findMetadata(map, key);
        if(metadata != null){
            //TODO: see if global tables can be queried from local always
            StreamConfiguration.HostAndPort hostAndPort = config.getHostAndPort();
            if(metadata.host().equals(hostAndPort.host) && metadata.port() == hostAndPort.port){
                byte[] bytes = queryLocal(map, key);
                return bytes != null ? Base64.getEncoder().encodeToString(bytes) : null;
            }
            else{
                return queryRemote(map, base64Key, metadata.hostInfo());
            }
        }
        return null;
    }

    private String queryRemote(String store, String base64Key, HostInfo hostInfo) {
        String url = "http://"+hostInfo.host()+":"+hostInfo.port() + SimpleHttpService.QUERY_PATH;
        url = url.replaceFirst(SimpleHttpService.ARG_STORE, store);
        url = url.replaceFirst(SimpleHttpService.ARG_KEY, base64Key);

        return edgeService.invokeGet(url);
    }

    private byte[] queryLocal(String store, byte[] key) {
        return findInLocal(store, key);
    }

    private static class KVStore{
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public ReadOnlyKeyValueStore<byte[], byte[]> getInstance() {
            return instance;
        }

        public void setInstance(ReadOnlyKeyValueStore<byte[], byte[]> instance) {
            this.instance = instance;
        }

        String name;
        ReadOnlyKeyValueStore<byte[], byte[]> instance;

        public KVStore(String name, ReadOnlyKeyValueStore<byte[], byte[]> instance) {
            this.name = name;
            this.instance = instance;
        }
    }

    /**
     * Get the metadata for all of the instances of this Kafka Streams application
     * @return List of {@link HostStoreInfo}
     */
    @Override
    public List<HostStoreInfo> metadata() {
        // Get metadata for all of the instances of this Kafka Streams application
        final Collection<StreamsMetadata> metadata = config.getGlobalStreamApp().allMetadata();
        return mapInstancesToHostStoreInfo(metadata);
    }
    /**
     * Find the metadata for the instance of this Kafka Streams Application that has the given
     * store and would have the given key if it exists.
     * @param map   Store to find
     * @param key     The key to find
     * @return {@link HostStoreInfo} or null if not found
     */
    @Override
    public <K> HostStoreInfo metadataForMapAndKey(final String map,
                                                  final String key
    ) {
        // Get metadata for the instances of this Kafka Streams application hosting the store and
        // potentially the value for key
        final StreamsMetadata metadata = findMetadata(map, Base64.getDecoder().decode(key));
        if (metadata == null) {
            return null;
        }

        return new HostStoreInfo(metadata.host(),
                metadata.port(),
                metadata.stateStoreNames());
    }
    private static List<HostStoreInfo> mapInstancesToHostStoreInfo(
            final Collection<StreamsMetadata> metadatas) {
        return metadatas.stream().map(metadata -> new HostStoreInfo(metadata.host(),
                metadata.port(),
                metadata.stateStoreNames()))
                .collect(Collectors.toList());
    }
    /**
     *
     * @param store
     * @param key
     * @return
     */
    StreamsMetadata findMetadata(String store, byte[] key){
        return config.getGlobalStreamApp().metadataForKey(store + StreamConfiguration.STORE_SUFFIX, key, Serdes.ByteArray().serializer());
    }
    /**
     *
     * @param store
     * @param key
     * @return
     */
    byte[] findInLocal(String store, byte[] key){
        ReadOnlyKeyValueStore<byte[], byte[]> keyValueStore = loadStore(store + StreamConfiguration.STORE_SUFFIX);
        if(keyValueStore != null){
            return keyValueStore.get(key);
        }
        return null;

    }

    private ReadOnlyKeyValueStore<byte[], byte[]> loadStore(String store) {
        /*
        if(config.getRunningStreams().containsKey(store) && config.getRunningStreams().get(store).state() == KafkaStreams.State.RUNNING){
            return config.getRunningStreams().get(store).store(store, QueryableStoreTypes.<byte[], byte[]>keyValueStore());
        }

         */
        if(config.getGlobalStreamApp().state() == KafkaStreams.State.RUNNING){
            return config.getGlobalStreamApp().store(store, QueryableStoreTypes.<byte[], byte[]>keyValueStore());
        }
        return null;
    }



}
