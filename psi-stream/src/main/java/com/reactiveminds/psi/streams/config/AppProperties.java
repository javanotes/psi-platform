package com.reactiveminds.psi.streams.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "psi.stream")
public class AppProperties {
    private Map<String, String> app = new HashMap<>();

    public Map<String, String> getApp() {
        return app;
    }

    public boolean isEnableCleanup() {
        return enableCleanup;
    }

    public void setEnableCleanup(boolean enableCleanup) {
        this.enableCleanup = enableCleanup;
    }

    private boolean enableCleanup = false;
    private String topic;
    private String store;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getStore() {
        return store;
    }

    public void setStore(String store) {
        this.store = store;
    }

    public void setApp(Map<String, String> app) {
        this.app = app;
    }
}
