package com.reactiveminds.psi.common;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class OperationSet implements Externalizable {
    private final List<KeyValue> ops = new ArrayList<>();
    public static final String TXN_MAP = "psi.txn.map";
    public static final String HEADER_TXN_ID = "psi.txn.id";
    public static final String HEADER_TXN_TTL = "psi.txn.ttl";
    public static final String HEADER_TXN_CHANNEL = "psi.txn.channel";
    public static final String HEADER_TXN_CHANNEL_PARTITION = "psi.txn.channel.part";
    public static final String HEADER_TXN_CHANNEL_OFFSET = "psi.txn.channel.off";
    public static final String HEADER_TXN_CLIENT_TYP = "psi.txn.client.typ";

    public void coalesce(){
        Collection<KeyValue> values = getOps().stream().collect(Collectors.toMap(KeyValue::getMap, Function.identity()))
                .values();

        getOps().clear();
        getOps().addAll(values);
    }

    public int participantCount(){
        return (int) getOps().stream().map(KeyValue::getMap).distinct().count();
    }
    public List<KeyValue> getOps() {
        return ops;
    }

    public String getTxnId() {
        return txnId;
    }

    public void setTxnId(String txnId) {
        this.txnId = txnId;
    }

    private String txnId;
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(getTxnId());
        out.writeInt(ops.size());
        for(KeyValue op: ops){
            op.writeExternal(out);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setTxnId(in.readUTF());
        int n = in.readInt();
        for (int i = 0; i < n; i++) {
            KeyValue kv = new KeyValue();
            kv.readExternal(in);
            ops.add(kv);
        }
    }

    public static class KeyValue implements Externalizable{
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            KeyValue keyValue = (KeyValue) o;
            return Arrays.equals(k, keyValue.k);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(k);
        }

        public static final byte OP_DEL = 0;
        public static final byte OP_SAVE = 1;
        byte[] k;
        byte[] v;
        byte op;
        String map;

        public KeyValue(byte[] k, byte[] v, byte op, String map) {
            this.k = k;
            this.v = v;
            this.op = op;
            this.map = map;
        }

        public KeyValue() {
        }

        public byte[] getK() {
            return k;
        }

        public void setK(byte[] k) {
            this.k = k;
        }

        public byte[] getV() {
            return v;
        }

        public void setV(byte[] v) {
            this.v = v;
        }

        public byte getOp() {
            return op;
        }

        public void setOp(byte op) {
            this.op = op;
        }

        public String getMap() {
            return map;
        }

        public void setMap(String map) {
            this.map = map;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(op);
            out.writeUTF(map);
            if(k != null){
                out.writeInt(k.length);
                out.write(k);
            }
            else
                out.writeInt(0);
            if(v != null){
                out.writeInt(v.length);
                out.write(v);
            }
            else
                out.writeInt(0);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            op = in.readByte();
            map = in.readUTF();
            int l = in.readInt();
            if(l > 0){
                byte[] b = new byte[l];
                in.readFully(b);
                k = b;
            }
            l = in.readInt();
            if(l > 0){
                byte[] b = new byte[l];
                in.readFully(b);
                v = b;
            }
        }
    }
}
