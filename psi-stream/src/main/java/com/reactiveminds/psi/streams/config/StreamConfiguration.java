package com.reactiveminds.psi.streams.config;

import com.reactiveminds.psi.common.imdg.DataGridConfiguration;
import com.reactiveminds.psi.common.kafka.KafkaClientsConfiguration;
import com.reactiveminds.psi.streams.processor.StreamStateProcessor;
import com.reactiveminds.psi.streams.processor.TransactionStateProcessor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Import({ DataGridConfiguration.class, KafkaClientsConfiguration.class})
@Configuration
@EnableConfigurationProperties({AppProperties.class})
public class StreamConfiguration {
    public static final String STORE_SUFFIX = ".store";
    public static final String REDO_LOG_SUFFIX = ".redo";
    private static final Logger log = LoggerFactory.getLogger(StreamConfiguration.class);
    @Autowired
    KafkaProperties kafkaProperties;
    @Autowired
    AppProperties appProperties;
    @Value("${psi.stream.queryListener.host:localhost}")
    private String advHost;
    @Value("${psi.stream.maxThreads:1}")
    private int threads;

    @Value("${psi.stream.queryListener.port:9999}")
    private int advPort;

    public HostStoreInfo localInstance(){
        return new HostStoreInfo(advHost, advPort);
    }

    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    @Bean
    StreamStateProcessor processor(String store, String name){
        return new StreamStateProcessor(store, name);
    }

    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    @Bean
    TransactionStateProcessor txnprocessor(){
        return new TransactionStateProcessor();
    }

    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    @Bean
    StoreBuilder storeBuilder(String storeName){
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(storeName),
                Serdes.ByteArray(),
                Serdes.ByteArray())
                .withLoggingDisabled(); // the kafka stream is the source of truth for us
    }

    @Bean
    AdminClient adminClient(@Autowired KafkaAdmin admin){
        return AdminClient.create(admin.getConfig());
    }

    //@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    @Bean
    KafkaStreams buildProcessorTopology(AdminClient admin)  {
        Properties props = new Properties();
        props.putAll(kafkaProperties.buildStreamsProperties());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, advHost + ":" + advPort);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Math.max(threads, appProperties.getApp().size()));
        // started
        props.putAll(kafkaProperties.buildConsumerProperties());
        // Set the commit interval to 500ms so that any changes are flushed frequently and the top five
        // charts are updated with low latency.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        final Topology builder = new Topology();

        Assert.isTrue(!appProperties.getApp().isEmpty(), "No `psi.stream.app.<event.topic.name>=<map name>` configuration found! Cannot run stream");

        appProperties.getApp().forEach((topic, map) -> {
            builder.addSource(topic, topic);
            try {
                admin.createTopics(Arrays.asList(new NewTopic(topic, 4, (short) 1))).all().get();
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                log.warn("on create topic "+topic, e.getCause());
            }
            builder.addProcessor("__processor."+map, new ProcessorSupplier() {
                @Override
                public Processor get() {
                    return beanFactory.getBean(StreamStateProcessor.class, map + STORE_SUFFIX, "__processor."+map);
                }
            }, topic);
            builder.addStateStore(storeBuilder(map + STORE_SUFFIX), "__processor."+map  );
            builder.addStateStore(storeBuilder(map + STORE_SUFFIX + REDO_LOG_SUFFIX), "__processor."+map  );
        });

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.error("** STREAM IS CLOSING DUE TO UNCAUGHT ERROR **", e);
            }
        });
        streams.setStateListener(new KafkaStreams.StateListener() {
            @Override
            public void onChange(KafkaStreams.State state, KafkaStreams.State state1) {

            }
        });

        log.info(builder.describe()+"");
        if(appProperties.getApp().size() > 1){
            log.warn("More than 1 stream.app mapping found. This is not recommended, as it might result in task-to-thread imbalance leading to " +
                    "transactions timing out unnecessarily. Tip: Check the number of tasks created and thread allotment from logs. There should be " +
                    "no thread allocated to more than 1 store\n");
        }
        return streams;
    }


    @PreDestroy
    void destroy(){
        globalStreamApp.close();
    }

    @Autowired
    BeanFactory beanFactory;

    @PostConstruct
    public void run()  {
        globalStreamApp = (KafkaStreams) beanFactory.getBean("buildProcessorTopology");
        if (appProperties.isEnableCleanup()) {
            globalStreamApp.cleanUp();
        }
        globalStreamApp.start();

    }

    public KafkaStreams getGlobalStreamApp() {
        return globalStreamApp;
    }

    private KafkaStreams globalStreamApp;
}
