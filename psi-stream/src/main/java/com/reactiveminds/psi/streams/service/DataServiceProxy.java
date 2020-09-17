package com.reactiveminds.psi.streams.service;

import com.reactiveminds.psi.streams.config.HostStoreInfo;
import com.reactiveminds.psi.streams.config.StreamConfiguration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Component
class DataServiceProxy implements StreamsDataService {

    @Autowired
    StreamConfiguration config;
    @Autowired
    SimpleHttpService edgeService;

    @Override
    public long count(String map) {
        List<HostStoreInfo> hostStoreInfos = metadata(map);
        HostStoreInfo local = null;
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        for (HostStoreInfo each: hostStoreInfos){
            if(isLocalInstance(each)){
                local = each;
            }
            else{
                futures.add(CompletableFuture.supplyAsync(() -> countRemote(map, each)) );
            }
        }
        long total = 0;
        if(local != null){
            total += localCount(map);
        }
        if(!futures.isEmpty()){
            total += futures.stream().map(CompletableFuture::join).mapToLong(l->l).sum();
        }
        return total;
    }

    private boolean isLocalInstance(HostStoreInfo one){
        return config.localInstance().equals(one);
    }
    private boolean isLocalInstance(StreamsMetadata one){
        return isLocalInstance(metadataToInfo(one));
    }

    @Override
    public String findByKey(String map, String base64Key){
        byte[] key = Base64.getDecoder().decode(base64Key);
        StreamsMetadata metadata = findMetadata(map, key);
        if(metadata != null){
            if(isLocalInstance(metadata) ){
                byte[] bytes = queryLocal(map, key);
                return bytes != null ? Base64.getEncoder().encodeToString(bytes) : null;
            }
            else{
                return queryRemote(map, base64Key, metadataToInfo(metadata));
            }
        }
        return null;
    }

    private String queryRemote(String store, String base64Key, HostStoreInfo hostInfo) {
        String url = "http://"+hostInfo.getHost()+":"+hostInfo.getPort() + SimpleHttpService.QUERY_PATH;
        url = url.replaceFirst(SimpleHttpService.ARG_STORE, store);
        url = url.replaceFirst(SimpleHttpService.ARG_KEY, base64Key);

        return edgeService.invokeGet(url);
    }
    private long countRemote(String store, HostStoreInfo hostInfo) {
        String url = "http://"+hostInfo.getHost()+":"+hostInfo.getPort() + SimpleHttpService.COUNT_PATH_LOCAL;
        url = url.replaceFirst(SimpleHttpService.ARG_STORE, store);

        return Long.parseLong(edgeService.invokeGet(url));
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
    public List<HostStoreInfo> metadata(String map) {
        Collection<StreamsMetadata> metadata = config.getGlobalStreamApp().allMetadataForStore(map + StreamConfiguration.STORE_SUFFIX);
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
        return metadatas.stream().map(metadata -> metadataToInfo(metadata))
                .collect(Collectors.toList());
    }
    private static HostStoreInfo metadataToInfo(StreamsMetadata metadata){
        return new HostStoreInfo(metadata.host(),
                metadata.port(),
                metadata.stateStoreNames());
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

    long localCount(String store){
        ReadOnlyKeyValueStore<byte[], byte[]> keyValueStore = loadStore(store + StreamConfiguration.STORE_SUFFIX);
        if(keyValueStore != null){
            return keyValueStore.approximateNumEntries();
        }
        return 0;
    }
    private ReadOnlyKeyValueStore<byte[], byte[]> loadStore(String store) {

        if(config.getGlobalStreamApp().state() == KafkaStreams.State.RUNNING){
            return config.getGlobalStreamApp().store(store, QueryableStoreTypes.<byte[], byte[]>keyValueStore());
        }
        return null;
    }



}
