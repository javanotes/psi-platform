package com.reactiveminds.psi.server;

import com.hazelcast.map.MapStore;
import com.reactiveminds.psi.common.OperationSet;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


class TransactionMapStore implements MapStore<String, OperationSet> {
    public TransactionMapStore(Properties p) {
        props = p;
    }
    private Properties props;
    private ExecutorService executor;
    @PostConstruct
    void init(){
        AtomicInteger a = new AtomicInteger(1);
        executor = Executors.newFixedThreadPool(Integer.parseInt(props.getProperty("manager.threads", "10")), r -> {
            Thread t = new Thread(r, "psi.Txn.Manager-"+a.getAndIncrement());
            return t;
        });
    }
    private String topic;

    @PreDestroy
    void destroy() throws InterruptedException {
        if (executor != null) {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Autowired
    BeanFactory beanFactory;

    @Override
    public void store(String s, OperationSet operationSet) {
        TransactionOrchestrator orchestrator = beanFactory.getBean(TransactionOrchestrator.class, operationSet);
        //orchestrator.setTxnTTL(TimeUnit.SECONDS.toMillis(Long.parseLong(props.getProperty("ttl")) ));
        orchestrator.initiateProtocol();
        executor.submit(orchestrator);
    }

    @Override
    public void storeAll(Map<String, OperationSet> map) {
        map.entrySet().forEach(e -> store(e.getKey(), e.getValue()));
    }

    @Override
    public void delete(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteAll(Collection<String> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public OperationSet load(String s) {
        return null;
    }

    @Override
    public Map<String, OperationSet> loadAll(Collection<String> collection) {
        return null;
    }

    @Override
    public Iterable<String> loadAllKeys() {
        return null;
    }
}
