package com.reactiveminds.psi.streams.processor;

import com.reactiveminds.psi.common.OperationSet;
import com.reactiveminds.psi.streams.config.AppProperties;
import com.reactiveminds.psi.streams.config.StreamConfiguration;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@Deprecated
public class TransactionStateProcessor implements Processor<byte[], byte[]> {
    private static final Logger log = LoggerFactory.getLogger(TransactionStateProcessor.class);
    private ProcessorContext processorContext;
    private List<String> storeList;

    private Map<String, KeyValueStore<byte[], byte[]>> storeMaps;
    public TransactionStateProcessor() {

    }
    @PostConstruct
    void init(){
        storeList = appProperties.getApp().values().stream().map(s -> s + StreamConfiguration.STORE_SUFFIX).collect(Collectors.toList());
        log.info(" all stores: {}", storeList);
    }
    @Autowired
    AppProperties appProperties;

    @Override
    public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;
        Map<String, KeyValueStore<byte[], byte[]>> collect = storeList.stream().map(store -> (KeyValueStore<byte[], byte[]>) processorContext.getStateStore(store))
                .collect(Collectors.toMap(ss -> ss.name(), Function.identity()));
        storeMaps = new ConcurrentHashMap<>(collect.size());
        storeMaps.putAll(collect);
        log.info("== Init complete ");
    }

    private static OperationSet unmarshall(byte[] v){
        OperationSet operationSet = new OperationSet();
        try(ObjectInputStream o = new ObjectInputStream(new ByteArrayInputStream(v))){
            operationSet.readExternal(o);
        } catch (IOException | ClassNotFoundException e) {
            throw new DataException(e);
        }
        return operationSet;
    }

    @Override
    public void process(byte[] k, byte[] v) {
        processTransaction(k,v);
        processorContext.commit();
        processorContext.forward(k,v);
    }

    private void processTransaction(byte[] k, byte[] v) {
        log.info("transactional message recieved");
        OperationSet operationSet = unmarshall(v);
        List<OperationSet.KeyValue> ops = operationSet.getOps();
        for(OperationSet.KeyValue op: ops){
            KeyValueStore<byte[], byte[]> store = storeMaps.get(op.getMap());
            if(store != null){

            }
            else{

            }
        }
    }

    @Override
    public void close() {

    }
}
