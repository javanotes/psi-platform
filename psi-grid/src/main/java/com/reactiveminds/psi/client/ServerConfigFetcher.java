package com.reactiveminds.psi.client;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.reactiveminds.psi.common.SerializableConfig;

import java.io.Serializable;
import java.util.concurrent.Callable;

class ServerConfigFetcher implements Callable<SerializableConfig>, HazelcastInstanceAware, Serializable {
    private HazelcastInstance instance;
    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.instance = hazelcastInstance;
    }

    @Override
    public SerializableConfig call() throws Exception {
        SerializableConfig config = new SerializableConfig();
        config.setConfiguredMaps(instance.getConfig().getMapConfigs().keySet());
        return config;
    }
}
