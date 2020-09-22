package com.reactiveminds.psi.common.imdg;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.reactiveminds.psi.common.DataWrapper;
import com.reactiveminds.psi.common.OperationSet;
import com.reactiveminds.psi.client.GridTransactionContext;
import com.reactiveminds.psi.common.err.GridOperationFailedException;
import com.reactiveminds.psi.common.err.GridTransactionException;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class TransactionContextProxy implements GridTransactionContext {
    private TransactionContext context;

    @Autowired
    HazelcastInstance hazelcastInstance;
    @Autowired
    SpecificAvroSerializer serializer;
    @Autowired
    SpecificAvroDeserializer deserializer;

    @Value("${psi.grid.client.transactionTimeoutSeconds:120}")
    long txnTimeoutSec;
    @Value("${psi.grid.client.twoPhaseCommit.enable:true}")
    boolean twoPhaseEnabled;

    TransactionContextProxy() {
    }
    @PostConstruct
    void init(){
        TransactionOptions options = new TransactionOptions()
                .setTransactionType( twoPhaseEnabled ? TransactionOptions.TransactionType.TWO_PHASE :  TransactionOptions.TransactionType.ONE_PHASE )
                .setTimeout(txnTimeoutSec, TimeUnit.SECONDS);

        context = hazelcastInstance.newTransactionContext( options );
    }

    private OperationSet mapKeys;
    @Override
    public void begin() {
        context.beginTransaction();
        mapKeys = new OperationSet();
        mapKeys.setTxnId(context.getTxnId().toString());
    }

    @Override
    public void commit() {
        try {
            context.commitTransaction();
            flushOperationSet();
            /**
             * It is possible to have inconsistent data across grid and store! The Hazelcast commit and write through are not
             * in a 'complete synchronous transaction'. So on an exception on write through, we are evicting the committed entries from grid.
             *
             * However, boundary condition - grid transaction is complete, write through exception is raise, but before the
             * eviction completes, the node goes down. So now the grid has the latest transaction, but not the backing store. To
             * avoid this scenario, we should have no backup nodes (?) - so that if node goes down, all in-memory state goes
             * down with it. What we are left with is the last committed state, as present in the backing store.
             *
             *
             */
        } catch (Throwable e) {
            evictCommitted();
            throw new GridTransactionException("Transaction rolled back! ", e);
        }
    }

    protected void flushOperationSet() {
        mapKeys.coalesce();
        GridOperationFailedException fe = new GridOperationFailedException("remote execution failed on 2pc");
        CountDownLatch l = new CountDownLatch(1);
        AtomicBoolean fail = new AtomicBoolean(false);
        hazelcastInstance.getExecutorService(OperationSet.TXN_MAP).submitToKeyOwner(new TransactionOrchestratorRunner(mapKeys), mapKeys.getTxnId(), new ExecutionCallback<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                l.countDown();
            }

            @Override
            public void onFailure(Throwable throwable) {
                fe.initCause(throwable);
                fail.set(true);
                l.countDown();
            }
        });
        try {
            l.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if(fail.get())
            throw  fe;
        //hazelcastInstance.getMap(OperationSet.TXN_MAP).set(context.getTxnId().toString(), mapKeys, 60, TimeUnit.SECONDS);
    }

    private void evictCommitted() {
        //we don't have a redo log. So simply evict from datagrid, and let it load from the persistent store on next get
        mapKeys.getOps().forEach(kv -> hazelcastInstance.getMap(kv.getMap()).evict(kv.getK()));
    }

    @Override
    public void rollback() {
        context.rollbackTransaction();
    }

    @Override
    public <K extends SpecificRecord, V extends SpecificRecord> V get(K key, String map) {
        byte[] bytes = serializer.serialize(null, key);
        DataWrapper o = (DataWrapper) context.getMap(map).get(bytes);
        if(o != null){
            return (V) deserializer.deserialize(null, o.getPayload());
        }
        return null;
    }

    @Override
    public <K extends SpecificRecord> boolean delete(K key, String map) {
        if(key == null)
            return false;
        byte[] k = serializer.serialize(null, key);
        context.getMap(map).delete(k);
        mapKeys.getOps().add(new OperationSet.KeyValue(k, null, OperationSet.KeyValue.OP_DEL, map));
        return true;
    }

    @Override
    public <K extends SpecificRecord, V extends SpecificRecord> V getForUpdate(K key, String map) {
        byte[] bytes = serializer.serialize(null, key);
        DataWrapper o = (DataWrapper) context.getMap(map).getForUpdate(bytes);
        if(o != null){
            return (V) deserializer.deserialize(null, o.getPayload());
        }
        return null;
    }

    @Override
    public <K extends SpecificRecord, V extends SpecificRecord> void put(K key, V value, String map) {
        byte[] k = serializer.serialize(null, key);
        byte[] v = serializer.serialize(null, value);
        context.getMap(map).set(k,new DataWrapper(true, v));
        mapKeys.getOps().add(new OperationSet.KeyValue(k, v, OperationSet.KeyValue.OP_SAVE, map));
    }


}
