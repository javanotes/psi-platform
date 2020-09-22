package com.reactiveminds.psi.server;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.reactiveminds.psi.SpringContextWrapper;
import com.reactiveminds.psi.common.BaseConfiguration;
import com.reactiveminds.psi.common.OperationSet;
import com.reactiveminds.psi.common.imdg.TransactionOrchestrator;
import com.reactiveminds.psi.common.kafka.KafkaClientsConfiguration;
import com.reactiveminds.psi.server.loaders.LoaderConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.core.io.Resource;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

@Import({KafkaClientsConfiguration.class, LoaderConfiguration.class, BaseConfiguration.class})
@ConditionalOnProperty(name = "run.mode", havingValue = "server", matchIfMissing = true)
@Configuration
public class ServerConfiguration {
    private static final Logger log = LoggerFactory.getLogger(ServerConfiguration.class);
    @Value("${spring.hazelcast.config:}")
    private String configXml;
    @Value("${spring.hazelcast.multicastPort:50071}")
    private int multicastPort;
    @Value("${spring.hazelcast.thisPort:0}")
    private int thisPort;
    @Value("${psi.grid.txnEntryTTLSecs:30}")
    private int txnEntryTTLSecs;
    @Value("${psi.grid.txn2PCTTLSecs:60}")
    private int txn2PCTTLSecs;


    @Autowired
    ApplicationContext context;


    @Autowired
    private ListableBeanFactory beanFactory;

    private static Config getConfig(Resource configLocation) throws IOException {
        if(configLocation == null)
            return new XmlConfigBuilder().build();

        URL configUrl = configLocation.getURL();
        Config config = new XmlConfigBuilder(configUrl).build();
        if (ResourceUtils.isFileURL(configUrl)) {
            config.setConfigurationFile(configLocation.getFile());
            log.info("CONFIG FILE: {}", configLocation.getFile());
        }
        else {
            config.setConfigurationUrl(configUrl);
            log.info("CONFIG URL: {}", configUrl);
        }
        return config;
    }

    @Bean
    SpringContextWrapper contextWrapper(){
        return new SpringContextWrapper();
    }
    @Bean
    SslServer sslServer(){
        return new SslServer();
    }

    @Lazy
    @Bean
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    DefaultMapStore mapStore(Properties p){
        return new DefaultMapStore(p);
    }

    @Lazy
    @Bean
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    TransactionMapStore txnMapStore(Properties p){
        return new TransactionMapStore(p);
    }

    @Bean
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    TransactionOrchestrator txnOrchestrator(OperationSet p){
        return new TransactionOrchestrator(p);
    }

    @PostConstruct
    void init(){
        /*
        if (instance instanceof HazelcastInstanceProxy) {
            HazelcastInstanceImpl original = ((HazelcastInstanceProxy) instance).getOriginal();
            ClientExceptions clientExceptions = original.node.getClientEngine().getClientExceptions();
            clientExceptions.register( USER_EXCEPTIONS_RANGE_START + 1, WriteThroughOperationException.class);
            clientExceptions.register( USER_EXCEPTIONS_RANGE_START + 2, ReadThroughOperationException.class);
        }

         */
        log.info("== Node running in SERVER mode ==");
        //sslServer().run();
    }

    @Bean
    public HazelcastInstance hazelcastInstance()
            throws IOException {
        Resource config = null;
        boolean hasConfigXml = false;
        if(StringUtils.hasText(configXml)) {
            config = context.getResource(configXml);
            hasConfigXml = true;
        }
        final Config conf = getConfig(config);
        conf.setProperty("hazelcast.rest.enabled", "false");

        if (!hasConfigXml) {
            NetworkConfig network = conf.getNetworkConfig();
            if (thisPort > 0) {
                network.setPort(thisPort);
            }
            if (multicastPort > 0) {
                JoinConfig join = network.getJoin();
                join.getTcpIpConfig().setEnabled(false);
                join.getAwsConfig().setEnabled(false);
                join.getMulticastConfig().setEnabled(true);
                join.getMulticastConfig().setMulticastPort(multicastPort);
            }
        }
        conf.getMapConfigs().entrySet().stream().filter(e -> !e.getKey().toLowerCase().equals("default"))
        .forEach(e -> {
            MapStoreConfig storeCfg = e.getValue().getMapStoreConfig();
            storeCfg.setProperty("map", e.getValue().getName());
            storeCfg.setEnabled(true);
            DefaultMapStore mapStore = mapStore(storeCfg.getProperties());
            storeCfg.setImplementation(mapStore);
            e.getValue().addEntryListenerConfig(new EntryListenerConfig(mapStore, true, true));
            storeCfg.setWriteDelaySeconds(0);//write-through
            e.getValue().setMapStoreConfig(storeCfg);
        });

        MapStoreConfig storeCfg = new MapStoreConfig();
        storeCfg.setProperty("map", OperationSet.TXN_MAP);
        storeCfg.setProperty("ttl", String.valueOf(txn2PCTTLSecs));
        storeCfg.setEnabled(true);
        storeCfg.setImplementation(txnMapStore(storeCfg.getProperties()));
        storeCfg.setWriteDelaySeconds(0);//write-through

        conf.getMapConfigs().put(OperationSet.TXN_MAP, new MapConfig(OperationSet.TXN_MAP));
        conf.getMapConfigs().get(OperationSet.TXN_MAP).setMapStoreConfig(storeCfg);
        conf.getMapConfigs().get(OperationSet.TXN_MAP).setTimeToLiveSeconds(txnEntryTTLSecs);

        return Hazelcast.newHazelcastInstance(conf);
    }
}
