package com.reactiveminds.psi.streams.service;

import com.reactiveminds.psi.streams.config.HostStoreInfo;

import java.util.List;

public interface StreamsDataService {
    long count(String map);
    /**
     *
     * @param map
     * @param base64Key
     * @return
     */
    String findByKey(String map, String base64Key);

    /**
     *
     * @return
     */
    List<HostStoreInfo> metadata(String map);

    /**
     *
     * @param map
     * @param key
     * @param <K>
     * @return
     */
    <K> HostStoreInfo metadataForMapAndKey(String map,
                                           String key
    );
}
