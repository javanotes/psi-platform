package com.reactiveminds.psi.common;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class DataWrapper implements Externalizable {
    private boolean isTransactional;
    private byte[] payload;

    public DataWrapper(byte[] payload) {
        this.payload = payload;
    }

    public DataWrapper(boolean isTransactional, byte[] payload) {
        this.isTransactional = isTransactional;
        this.payload = payload;
    }

    public DataWrapper(){}
    public boolean isTransactional() {
        return isTransactional;
    }

    public void setTransactional(boolean transactional) {
        isTransactional = transactional;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(isTransactional);
        if(payload != null) {
            out.writeInt(payload.length);
            out.write(payload);
        }
        else
            out.writeInt(0);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        isTransactional = in.readBoolean();
        payload = new byte[in.readInt()];
        in.readFully(payload);
    }
}
