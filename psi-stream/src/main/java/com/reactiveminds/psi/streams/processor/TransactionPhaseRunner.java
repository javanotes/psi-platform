package com.reactiveminds.psi.streams.processor;

import com.reactiveminds.psi.common.TwoPCConversation;
import com.reactiveminds.psi.common.TwoPhase;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class TransactionPhaseRunner implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TransactionPhaseRunner.class);
    private final TwoPCConversation twoPhaseConverse;
    private final long ttl;

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    private String storeName;

    public void setStateStore(KeyValueStore<byte[], byte[]> stateStore) {
        this.stateStore = stateStore;
    }

    public void setRedoLogStore(KeyValueStore<byte[], byte[]> redoLogStore) {
        this.redoLogStore = redoLogStore;
    }

    private KeyValueStore<byte[], byte[]> stateStore;
    private KeyValueStore<byte[], byte[]> redoLogStore;
    private final byte[] k;
    private final byte[] v;

    public TransactionPhaseRunner(TwoPCConversation twoPhaseConverse, long ttl, byte[] k, byte[] v) {
        this.twoPhaseConverse = twoPhaseConverse;
        this.ttl = ttl;
        this.k = k;
        this.v = v;
    }

    private void commit(byte[] k, byte[] v){
        if(v == null){
            stateStore.delete(k);
            log.debug("DELETE");
        }
        else{
            stateStore.put(k,v);
            log.debug("SAVE");
        }
    }

    @Override
    public void run() {
        try {
            Assert.notNull(stateStore, "state store not set");
            Assert.notNull(redoLogStore, "redo log store not set");

            String command = twoPhaseConverse.listen(ttl, TimeUnit.MILLISECONDS);
            if(TwoPhase.PREPARE.equals(command)){
                boolean proceed = false;
                byte[] previous = null;
                try {
                    previous = stateStore.get(k);
                    redoLogStore.put(k, previous);
                    twoPhaseConverse.tell(TwoPhase.PREPARE_ACK+":"+storeName);
                    log.debug("Prepare Ack Txn: {}, for {}", twoPhaseConverse.getCorrKey(), storeName);
                    proceed = true;
                } catch (Exception e) {
                    log.error("Aborting Txn: {}, on prepare fail", twoPhaseConverse.getCorrKey());
                    log.error("", e);
                    twoPhaseConverse.send(TwoPhase.PREPARE_NACK+":"+storeName);
                }
                if(proceed){
                    command = twoPhaseConverse.listen(ttl, TimeUnit.MILLISECONDS);
                    if(TwoPhase.COMMIT.equals(command)){
                        try {
                            commit(k, v);
                            log.info("Commit Txn: {}, for {}", twoPhaseConverse.getCorrKey(), storeName);
                            // TODO: point of failure - if app goes down at this line
                            twoPhaseConverse.tell(TwoPhase.COMMIT_ACK+":"+storeName);
                            proceed = true;
                        } catch (Exception e) {
                            log.error("Aborting Txn: {}, on commit fail, for {}", twoPhaseConverse.getCorrKey(), storeName);
                            log.error("", e);
                            twoPhaseConverse.send(TwoPhase.COMMIT_NACK+":"+storeName);
                            proceed = false;
                        }
                    }
                    else {
                        log.error("Not commiting Txn: {}, on command {}, for {}", twoPhaseConverse.getCorrKey(), command, storeName);
                        proceed = false;
                    }
                }
                if(proceed){
                    try {
                        command = twoPhaseConverse.listen(ttl, TimeUnit.MILLISECONDS);
                        if(!TwoPhase.END.equals(command)){
                            log.error("Rolling back Txn: {}, on command {}, for {} ", twoPhaseConverse.getCorrKey(), command, storeName);
                            proceed = false;
                        }

                    } catch (Exception e) {
                        log.error("Rolling back Txn: {}, on end for {}", twoPhaseConverse.getCorrKey(), storeName);
                        log.error("", e);
                        proceed = false;
                    }
                }
                if(!proceed){
                    commit(k, previous);
                }
            }
            else{
                log.error("Skipping Txn: {}, on unexpected first command {}, for {}. No ACK/NACK will be sent", twoPhaseConverse.getCorrKey(), command, storeName);
            }
        }
        catch (TimeoutException e) {
            if(e.getCause() == null){
                log.error("Aborting Txn: {}, on manager command timing out, for {}", twoPhaseConverse.getCorrKey(), storeName);
                twoPhaseConverse.send(TwoPhase.PREPARE_NACK+":"+storeName);
            }
            else{
                log.error("Rejecting Txn: {}, on unexpected error for {}! No ACK/NACK will be sent", twoPhaseConverse.getCorrKey(), storeName);
                log.error("", e.getCause());
            }
        }
        catch (Throwable e) {
            log.error("Rejecting Txn: {}, on unknown error for {}! No ACK/NACK will be sent", twoPhaseConverse.getCorrKey(), storeName);
            log.error("", e);
        }
        finally {
            redoLogStore.delete(k);
        }
    }
}
