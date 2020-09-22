package com.reactiveminds.psi.common.imdg;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.reactiveminds.psi.client.ClientConfiguration;
import com.reactiveminds.psi.common.BaseConfiguration;
import com.reactiveminds.psi.common.err.GridOperationNotAllowedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;
import org.springframework.util.StringUtils;

import javax.annotation.PreDestroy;
import java.io.IOException;

@Import(BaseConfiguration.class)
@Configuration
public class DataGridConfiguration {
    private static final Logger log = LoggerFactory.getLogger(ClientConfiguration.class);
    @Value("${spring.hazelcast.config:}")
    String configXml;
    @Autowired
    ApplicationContext context;

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

    private static HazelcastInstance getHazelcastInstance(ClientConfig clientConfig) {
        if (StringUtils.hasText(clientConfig.getInstanceName())) {
            return HazelcastClient
                    .getHazelcastClientByName(clientConfig.getInstanceName());
        }
        return HazelcastClient.newHazelcastClient(clientConfig);
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
