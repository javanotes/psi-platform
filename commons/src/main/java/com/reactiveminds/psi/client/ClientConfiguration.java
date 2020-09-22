package com.reactiveminds.psi.client;

import com.hazelcast.client.impl.clientside.ClientExceptionFactory;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.core.HazelcastInstance;
import com.reactiveminds.psi.common.SerializableConfig;
import com.reactiveminds.psi.common.err.GridOperationNotAllowedException;
import com.reactiveminds.psi.common.err.ReadThroughOperationException;
import com.reactiveminds.psi.common.err.WriteThroughOperationException;
import com.reactiveminds.psi.common.imdg.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.*;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Import(DataGridConfiguration.class)
@EnableConfigurationProperties(KafkaProperties.class)
@ConditionalOnProperty(name = "run.mode", havingValue = "client")
@Configuration
public class ClientConfiguration {
    private static final Logger log = LoggerFactory.getLogger(ClientConfiguration.class);

    @Autowired
    private KafkaProperties kafkaProperties;

    @Autowired
    HazelcastInstance hazelcastInstance;
    private SerializableConfig fetchServerConfig(HazelcastInstance instance) throws ExecutionException, InterruptedException {
        Future<SerializableConfig> config = instance.getExecutorService("fetchServerConfig").submit(new ServerConfigFetcher());
        return config.get();
    }
    @Lazy
    @Bean
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    GridOperation gridOperations(String map) throws IOException, ExecutionException, InterruptedException {
        SerializableConfig serverConfig = fetchServerConfig(hazelcastInstance);
        if(!serverConfig.getConfiguredMaps().contains(map))
            throw new GridOperationNotAllowedException("Unconfigured map! '"+map+"'. To perform grid operations this map needs to be configured on server nodes");
        return new GridTemplate(map);
    }
    @Bean
    GridTransaction gridTransaction(){
        return new GridTransactionTemplate();
    }

    @PostConstruct
    void init(){
        if (hazelcastInstance instanceof HazelcastClientProxy) {
            HazelcastClientProxy original = ((HazelcastClientProxy) hazelcastInstance);
            original.client.getClientExceptionFactory().register(5001, WriteThroughOperationException.class, new ClientExceptionFactory.ExceptionFactory() {
                @Override
                public Throwable createException(String s, Throwable throwable) {
                    return new WriteThroughOperationException(s, throwable);
                }
            });
            original.client.getClientExceptionFactory().register(5002, ReadThroughOperationException.class, new ClientExceptionFactory.ExceptionFactory() {
                @Override
                public Throwable createException(String s, Throwable throwable) {
                    return new ReadThroughOperationException(s, throwable);
                }
            });
        }
        log.info("== Node running in CLIENT mode ==");
    }


    @Lazy
    @Bean
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    GridTransactionContext transactionContext(){
        return new TransactionContextProxy();
    }


}
