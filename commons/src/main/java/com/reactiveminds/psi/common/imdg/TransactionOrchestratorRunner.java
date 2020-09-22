package com.reactiveminds.psi.common.imdg;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.reactiveminds.psi.SpringContextWrapper;
import com.reactiveminds.psi.common.OperationSet;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

public class TransactionOrchestratorRunner implements Runnable, Serializable, HazelcastInstanceAware {
    private final OperationSet operationSet;

    public TransactionOrchestratorRunner(OperationSet operationSet) {
        this.operationSet = operationSet;
    }

    public TransactionOrchestratorRunner(OperationSet operationSet, boolean synchRunMode) {
        this.operationSet = operationSet;
        this.synchRunMode = synchRunMode;
    }

    private boolean synchRunMode = false;
    @Override
    public void run() {
        TransactionOrchestrator orchestrator = SpringContextWrapper.getBean(TransactionOrchestrator.class, operationSet);
        orchestrator.initiateProtocol();
        // execute on local member
        if (!synchRunMode) {
            ThreadPoolTaskExecutor taskExecutor = SpringContextWrapper.getBean("txnTaskExecutor");
            taskExecutor.execute(orchestrator);
        }
        else{
            orchestrator.run();
        }
    }

    private HazelcastInstance hazelcastInstance;
    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }
}
