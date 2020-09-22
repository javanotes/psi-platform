package com.reactiveminds.psi.common;

import com.reactiveminds.psi.common.imdg.GridConversationalClient;
import com.reactiveminds.psi.common.kafka.tools.ConversationalClient;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class BaseConfiguration {

    @Value("${psi.grid.txnExecutor.maxPoolSize:20}")
    private int maxPoolSize;
    @Value("${psi.grid.txnExecutor.corePoolSize:10}")
    private int corePoolSize;
    @Value("${psi.grid.txnExecutor.queueCapacity:0}")
    private int queueCapacity;
    @Value("${psi.grid.txnExecutor.keepAliveSeconds:60}")
    private int keepAliveSeconds;
    @Bean
    ThreadPoolTaskExecutor txnTaskExecutor(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setMaxPoolSize(maxPoolSize);
        executor.setCorePoolSize(corePoolSize);
        if (queueCapacity > 0) {
            executor.setQueueCapacity(queueCapacity);
        }
        executor.setAllowCoreThreadTimeOut(false);
        executor.setKeepAliveSeconds(keepAliveSeconds);
        executor.setThreadNamePrefix("PSI.Txn.Mgr-");
        return executor;
    }
    @Bean
    TwoPCConversationClientFactory conversationClientFactory(){
        return new TwoPCConversationClientFactory();
    }

    /**
     *
     * @param topic
     * @param txnId
     * @return
     */
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    @Bean
    ConversationalClient conversationLeader(String topic, String txnId){
        return new ConversationalClient(topic, txnId);
    }

    /**
     *
     * @param topic
     * @param singlePartition
     * @param txnId
     * @param fromOffset
     * @return
     */
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    @Bean
    ConversationalClient conversationFollower(String topic, String txnId, int singlePartition, long fromOffset){
        return new ConversationalClient(new TopicPartition(topic, singlePartition), txnId, fromOffset);
    }
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    @Bean
    GridConversationalClient conversationLeader2(String txnId){
        return new GridConversationalClient(txnId);
    }

    /**
     *
     * @param txnId
     * @param fromOffset
     * @return
     */
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    @Bean
    GridConversationalClient conversationFollower2(String txnId, long fromOffset){
        return new GridConversationalClient(txnId, fromOffset);
    }
}
