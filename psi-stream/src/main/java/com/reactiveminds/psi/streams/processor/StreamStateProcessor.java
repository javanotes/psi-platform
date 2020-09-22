package com.reactiveminds.psi.streams.processor;

import com.reactiveminds.psi.common.OperationSet;
import com.reactiveminds.psi.common.TwoPCConversation;
import com.reactiveminds.psi.common.TwoPCConversationClientFactory;
import com.reactiveminds.psi.common.kafka.tools.ConversationalClient;
import com.reactiveminds.psi.common.util.SerdeUtils;
import com.reactiveminds.psi.streams.config.AppProperties;
import com.reactiveminds.psi.streams.config.StreamConfiguration;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.util.Assert;

public class StreamStateProcessor implements Processor<byte[], byte[]>, CommitProcessor {

    private static final Logger log = LoggerFactory.getLogger(StreamStateProcessor.class);
    private ProcessorContext processorContext;
    private final String storeName;
    private final String name;

    public StreamStateProcessor(String storeName, String name) {
        this.storeName = storeName;
        this.name = name;
    }

    @Autowired
    AppProperties appProperties;

    @Autowired
    TwoPCConversationClientFactory beanFactory;

    private KeyValueStore<byte[], byte[]> stateStore;
    private KeyValueStore<byte[], byte[]> redoLogStore;
    @Override
    public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;
        stateStore = (KeyValueStore<byte[], byte[]>)processorContext.getStateStore(storeName);
        redoLogStore = (KeyValueStore<byte[], byte[]>)processorContext.getStateStore(storeName+ StreamConfiguration.REDO_LOG_SUFFIX);
        flushRedoLog();
        log.info("Task {} -> Init complete for {}, with mapped store: {}",  processorContext.taskId(), name, storeName);
    }

    protected void flushRedoLog() {
        long entries = redoLogStore.approximateNumEntries();
        if(entries > 0){
            log.warn("Flushing redo log having {} entries, for store: {}. Will be skipped on a reload (?{})", entries, storeName, appProperties.isEnableCleanup());
            KeyValueIterator<byte[], byte[]> iterator = redoLogStore.all();
            try {
                while (iterator.hasNext()){
                    KeyValue<byte[], byte[]> keyValue = iterator.next();
                    if (!appProperties.isEnableCleanup()) {
                        stateStore.put(keyValue.key, keyValue.value);
                    }
                    redoLogStore.delete(keyValue.key);
                }
                redoLogStore.flush();
                stateStore.flush();
                entries = redoLogStore.approximateNumEntries();
                Assert.isTrue(entries == 0, "redo log table not cleared! count="+entries);
            }
            finally {
                iterator.close();
            }
        }
    }

    private boolean isTransactional(){
        Headers headers = processorContext.headers();
        Header header = headers.lastHeader(OperationSet.HEADER_TXN_ID);
        boolean txn = header != null;
        if(txn){
            log.debug("Got transaction id: {}", SerdeUtils.bytesToString(header.value()));
        }
        return txn;
    }
    private TwoPCConversation getConversationClient(){
        Headers headers = processorContext.headers();
        Header header = headers.lastHeader(OperationSet.HEADER_TXN_ID);
        String txnId = SerdeUtils.bytesToString(header.value());
        header = headers.lastHeader(OperationSet.HEADER_TXN_CHANNEL);
        String topic = SerdeUtils.bytesToString(header.value());
        header = headers.lastHeader(OperationSet.HEADER_TXN_CHANNEL_PARTITION);
        int part = SerdeUtils.bytesToInt(header.value());
        header = headers.lastHeader(OperationSet.HEADER_TXN_CHANNEL_OFFSET);
        long offset = SerdeUtils.bytesToLong(header.value());
        header = headers.lastHeader(OperationSet.HEADER_TXN_CLIENT_TYP);

        TwoPCConversationClientFactory.Type type = TwoPCConversationClientFactory.Type.valueOf(SerdeUtils.bytesToString(header.value()));
        return beanFactory.getFollower(type, topic, txnId, part, offset);
    }
    @Override
    public void process(byte[] k, byte[] v) {
        log.debug("received message from, {}-[{}]-[{}]", processorContext.topic(), processorContext.partition(), processorContext.offset());
        if(!appProperties.isEnableCleanup() && isTransactional()){
            processTransactional(k,v);
        }
        else {
            commit(k,v);
            log.debug("Processed key/value data commit for key len {}, value len {}", k.length, v != null ? v.length : 0);
        }
        processorContext.commit();
    }

    @Override
    public void commit(byte[] k, byte[] v){
        if(v == null){
            stateStore.delete(k);
            log.info("DELETE");
        }
        else{
            stateStore.put(k,v);
            log.info("SAVE");
        }
    }
    @Autowired
    TaskExecutor taskExecutor;
    private void processTransactional(byte[] k, byte[] v) {
        TwoPCConversation twoPhaseConverse = getConversationClient();
        Headers headers = processorContext.headers();
        Header header = headers.lastHeader(OperationSet.HEADER_TXN_TTL);
        long ttl = SerdeUtils.bytesToLong(header.value());

        TransactionPhaseRunner phaseRunner = new TransactionPhaseRunner(twoPhaseConverse, ttl, k, v);
        phaseRunner.setStoreName(storeName);
        phaseRunner.setStateStore(stateStore);
        phaseRunner.setRedoLogStore(redoLogStore);
        taskExecutor.execute(phaseRunner);

    }

    @Override
    public void close() {

    }
}
