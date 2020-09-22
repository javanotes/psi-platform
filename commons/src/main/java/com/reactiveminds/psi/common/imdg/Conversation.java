package com.reactiveminds.psi.common.imdg;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Conversation implements DataSerializable {
    private String key;
    private String value;
    private boolean master;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public boolean isMaster() {
        return master;
    }

    public void setMaster(boolean master) {
        this.master = master;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(getKey());
        objectDataOutput.writeUTF(getValue());
        objectDataOutput.writeBoolean(isMaster());
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        setKey(objectDataInput.readUTF());
        setValue(objectDataInput.readUTF());
        setMaster(objectDataInput.readBoolean());
    }
}
