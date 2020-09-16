package com.reactiveminds.psi.server;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.reactiveminds.psi.common.OperationSet;
import com.reactiveminds.psi.common.SerdeUtils;
import com.reactiveminds.psi.common.TwoPhase;
import com.reactiveminds.psi.common.err.GridTransactionException;
import com.reactiveminds.psi.common.kafka.tools.ConversationalClient;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

class TransactionOrchestrator implements Runnable{
    private final OperationSet operationSet;

    @Value("${psi.grid.2pc.enable:false}")
    boolean twoPhaseEnabled;

    @Value("${psi.grid.2pc.channel.topic:__psi.txn.channel}")
    private String txnChannel;

    @Value("${psi.grid.2pc.channel.conversationTimeoutMillis:180000}")
    private long txnChannelTimeout;

    public long getTxnTTL() {
        return txnTTL;
    }

    public void setTxnTTL(long txnTTL) {
        this.txnTTL = txnTTL;
    }

    private long txnTTL;
    public TransactionOrchestrator(OperationSet operationSet) {
        this.operationSet = operationSet;
    }

    @Autowired
    KafkaTemplate<byte[], byte[]> producer;
    @Autowired
    HazelcastInstance instance;

    private volatile boolean commenced = false;
    private Map<String, String> mapStoreTopic;
    @PostConstruct
    void init(){
        Map<String, MapConfig> mapConfigs = instance.getConfig().getMapConfigs();
        mapStoreTopic = mapConfigs.entrySet().stream().map(e -> e.getValue())
                .map(c -> c.getMapStoreConfig())
                .filter(m -> m.getProperties().containsKey("map") && m.getProperties().containsKey("event.topic.name"))
                .collect(Collectors.toMap(m -> m.getProperty("map"), m -> m.getProperty("event.topic.name")));
    }

    /**
     * Trigger a new 2 phase commit process. The prepare() phase is completed when this method would return.
     * The remaining part of the protocol will execute in a separate thread. So this is kind of an eventual
     * 2 phase commit.
     *
     *
     *          * 1. Manager sends PREPARE message to participants (this method)
     *          * 2. Each participant prepares
     *          *          - Copy the current local key/value state from persistent store to memory
     *          *          - Blocks the current consumer thread (this implies the txn TTL should be less than Kafka consumer heartbeat interval)
     *          *          - Send PREPARE_ACK to TXN_CHANNEL
     *          *          - Await COMMIT or ABORT signal from manager
     *          * 3. Manager awaits for PREPARE_ACK from all participants
     *          *          - If ACK received from all participants within TTL
     *          *              - send COMMIT message to all participants
     *          *              - Await COMMIT_ACK or COMMIT_NACK message from participants
     *          *          - Else send ABORT message to all participants and end transaction
     *          * 4. Participant awaiting COMMIT or ABORT signal from manager
     *          *          - If COMMIT received,
     *          *              - if committed send COMMIT_ACK to manager and await END* signal from manager
     *          *              - Else send COMMIT_NACK to manager
     *          *          - If ABORT received, abort and continue
     *          * 5. Manager awaits COMMIT_ACK or COMMIT_FAIL for all participants
     *          *          - If COMMIT_ACK send END
     *          *          - Else send ABORT
     *          * 6. Participant awaiting END or ABORT
     *          *          - If END then continue
     *          *          - If ABORT then rollback and continue
     *          *
     *          * All messages to TXN_CHANNEL are sent with the same key, so that they are ordered (fall in same partition)
     *          *
     *          * Manager              Participant
     *          * =======              ===========
     *          * PREPARE -->>
     *          *                   <<--PREPARE_ACK/PREPARE_NACK
     *          * COMMIT/ABORT-->>
     *          *                   <<--COMMIT_ACK/COMMIT_NACK
     *          * END/ABORT-->>
     *
     */
    private void applyTwoPhase(){
        log.info("Txn: {} preparing 2PC ", operationSet.getTxnId());
        twoPhaseConverse = (ConversationalClient) beanFactory.getBean("conversationLeader", txnChannel, operationSet.getTxnId());
        twoPhaseConverse.begin(TwoPhase.PREPARE);
        for(OperationSet.KeyValue op: operationSet.getOps()){
            try {

                ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(mapStoreTopic.get(op.getMap()), op.getK(),
                        op.getOp() == OperationSet.KeyValue.OP_SAVE ? op.getV() : null);

                rec.headers().add(OperationSet.HEADER_TXN_ID, SerdeUtils.stringToBytes(operationSet.getTxnId()));
                rec.headers().add(OperationSet.HEADER_TXN_CHANNEL, SerdeUtils.stringToBytes(txnChannel));
                rec.headers().add(OperationSet.HEADER_TXN_CHANNEL_PARTITION, SerdeUtils.intToBytes(twoPhaseConverse.getPartition()));
                rec.headers().add(OperationSet.HEADER_TXN_CHANNEL_OFFSET, SerdeUtils.longToBytes(twoPhaseConverse.getWriteOffset()));
                rec.headers().add(OperationSet.HEADER_TXN_TTL, SerdeUtils.longToBytes(txnChannelTimeout));
                producer.send(rec).get();
                log.debug("Txn: {}, published to event topic - {}. 2pc begin read offset {}", operationSet.getTxnId(), mapStoreTopic.get(op.getMap()), twoPhaseConverse.getReadOffset());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                twoPhaseConverse.send(TwoPhase.ABORT);
                throw new GridTransactionException("InterruptedException");
            } catch (ExecutionException e) {
                twoPhaseConverse.send(TwoPhase.ABORT);
                throw new GridTransactionException("Transaction write through failure", e.getCause());
            }
        }
        commenced = true;
    }
    private ConversationalClient twoPhaseConverse;

    public void initiateProtocol(){
        boolean match = operationSet.getOps().stream().anyMatch(kv -> !mapStoreTopic.containsKey(kv.getMap()));
        if(match)
            throw new GridTransactionException("Fatal! transaction operation set contains unmapped configuration. Configured map -> topic : "+ mapStoreTopic);

        if(operationSet.getOps().isEmpty()){
            log.warn("* Skipping Txn# {}. Empty operation set!", operationSet.getTxnId());
            return;
        }
        if (twoPhaseEnabled) {
            applyTwoPhase();
        }
        else
            applyOnePhase();
    }

    private void applyOnePhase() {
        log.info("Txn: {} starting 1PC ", operationSet.getTxnId());
        for(OperationSet.KeyValue op: operationSet.getOps()){
            try {

                ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(mapStoreTopic.get(op.getMap()), op.getK(),
                        op.getOp() == OperationSet.KeyValue.OP_SAVE ? op.getV() : null);

                producer.send(rec).get();
                log.debug("Txn: {}, published to event topic - {}. ", operationSet.getTxnId(), mapStoreTopic.get(op.getMap()));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new GridTransactionException("InterruptedException");
            } catch (ExecutionException e) {
                throw new GridTransactionException("Transaction write through failure", e.getCause());
            }
        }
        log.info("Txn: {} publish successful ", operationSet.getTxnId());
    }

    @Autowired
    BeanFactory beanFactory;
    @Override
    public void run() {
        try {
            if(commenced){
                run2pc();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (twoPhaseConverse != null) {
                    twoPhaseConverse.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(TransactionOrchestrator.class);
    private void run2pc() {
        int n = operationSet.participantCount();
        log.debug("No of participants {}, for txn# {}", n, operationSet.getTxnId());
        String response = null;
        try {
            boolean proceed = true;
            for (int i = 0; i < n; i++) {
                response = twoPhaseConverse.listen(txnChannelTimeout, TimeUnit.MILLISECONDS);
                log.debug("Txn# {}, phase 1 response: {}",operationSet.getTxnId(), response);
                if(!response.startsWith(TwoPhase.PREPARE_ACK)){
                    log.error("Aborting Txn: {}, on participant response {}", operationSet.getTxnId(), response);
                    twoPhaseConverse.tell(TwoPhase.ABORT);
                    proceed = false;
                    break;
                }
            }

            if(proceed){
                twoPhaseConverse.tell(TwoPhase.COMMIT);
                for (int i = 0; i < n; i++) {
                    response = twoPhaseConverse.listen(txnChannelTimeout, TimeUnit.MILLISECONDS);
                    log.debug("Txn# {}, phase 2 response: {}",operationSet.getTxnId(), response);
                    if(!response.startsWith(TwoPhase.COMMIT_ACK)){
                        log.error("Aborting Txn: {}, on participant response {}", operationSet.getTxnId(), response);
                        twoPhaseConverse.tell(TwoPhase.ABORT);
                        proceed = false;
                        break;
                    }
                }
            }

            if(proceed) {
                twoPhaseConverse.tell(TwoPhase.END);
                log.info("Txn: {} commit successful ", operationSet.getTxnId());
            }

        }
        catch (TimeoutException e) {
            if(e.getCause() == null){
                log.error("Aborting Txn: {}, on participant response timing out", operationSet.getTxnId());
                twoPhaseConverse.tell(TwoPhase.ABORT);
            }
            else{
                log.error("Rejecting Txn: {}, on unexpected error! No ABORT/END message will be sent", operationSet.getTxnId());
                log.error("", e.getCause());
            }
        }
        catch (Throwable e) {
            log.error("Rejecting Txn: {}, on unknown error! No ABORT/END message will be sent", operationSet.getTxnId());
            log.error("", e);
        }
    }

}
