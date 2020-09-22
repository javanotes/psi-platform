package com.reactiveminds.psi.common.imdg;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.impl.clientside.ClientExceptionFactory;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.core.HazelcastInstance;
import com.reactiveminds.psi.common.SerializableConfig;
import com.reactiveminds.psi.common.TwoPCConversationClientFactory;
import com.reactiveminds.psi.client.*;
import com.reactiveminds.psi.common.err.GridOperationNotAllowedException;
import com.reactiveminds.psi.common.err.ReadThroughOperationException;
import com.reactiveminds.psi.common.err.WriteThroughOperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.core.io.Resource;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Import(TwoPCConversationClientFactory.class)
@EnableConfigurationProperties(KafkaProperties.class)
@ConditionalOnProperty(name = "run.mode", havingValue = "client", matchIfMissing = true)
@Configuration
public class ClientConfiguration {
    private static final Logger log = LoggerFactory.getLogger(ClientConfiguration.class);
    @Value("${spring.hazelcast.config:}")
    String configXml;
    @Autowired
    ApplicationContext context;
    @Autowired
    private KafkaProperties kafkaProperties;
    @Value("${psi.grid.client.authEnabled:false}")
    boolean authEnabled;
    @Bean
    public HazelcastInstance hazelcastInstance()
            throws IOException {
        if (authEnabled) {
            Boolean auth = sslClient().get();
            if(!auth){
                throw new GridOperationNotAllowedException("SSL auth failed! Please install server certificate " +
                        "with `-Djavax.net.ssl.trustStore=<keystore> -Djavax.net.ssl.trustStorePassword=<password>`");
            }
        }
        Resource config = null;
        if(StringUtils.hasText(configXml)) {
            config = context.getResource(configXml);
        }
        ClientConfig c = config != null ? new XmlClientConfigBuilder(config.getURL()).build() : new XmlClientConfigBuilder().build();
        c.getNetworkConfig().setSmartRouting(true);
        return getHazelcastInstance(c);
    }

    private SerializableConfig fetchServerConfig(HazelcastInstance instance) throws ExecutionException, InterruptedException {
        Future<SerializableConfig> config = instance.getExecutorService("fetchServerConfig").submit(new ServerConfigFetcher());
        return config.get();
    }
    @Lazy
    @Bean
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    GridOperation gridOperations(String map) throws IOException, ExecutionException, InterruptedException {
        SerializableConfig serverConfig = fetchServerConfig(hazelcastInstance());
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
        if (client instanceof HazelcastClientProxy) {
            HazelcastClientProxy original = ((HazelcastClientProxy) client);
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
    private static HazelcastInstance getHazelcastInstance(ClientConfig clientConfig) {
        if (StringUtils.hasText(clientConfig.getInstanceName())) {
            return HazelcastClient
                    .getHazelcastClientByName(clientConfig.getInstanceName());
        }
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    @Lazy
    @Bean
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    GridTransactionContext transactionContext(){
        return new TransactionContextProxy();
    }

    @Autowired
    HazelcastInstance client;

    @PreDestroy
    void shutdown(){
        client.shutdown();
    }
    @Bean
    SslClient sslClient(){
        return new SslClient();
    }
}
