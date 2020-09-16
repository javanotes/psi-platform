package com.reactiveminds.psi.common;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class SerializableConfig implements Serializable {
    public Set<String> getConfiguredMaps() {
        return configuredMaps;
    }

    public void setConfiguredMaps(Set<String> configuredMaps) {
        this.configuredMaps.addAll(configuredMaps);
    }

    private Set<String> configuredMaps = new HashSet<>();


}
