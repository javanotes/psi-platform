package com.reactiveminds.psi.streams.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class HostStoreInfo {
    String host;

    public HostStoreInfo() {
    }

    public HostStoreInfo(String host, int port, Set<String> stores) {
        this.host = host;
        this.port = port;
        this.stores = new ArrayList<>(stores);
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public List<String> getStores() {
        return stores;
    }

    public void setStores(List<String> stores) {
        this.stores = stores;
    }

    int port;
    List<String> stores;
}
