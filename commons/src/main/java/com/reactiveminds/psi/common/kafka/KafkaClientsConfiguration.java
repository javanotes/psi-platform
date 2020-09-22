package com.reactiveminds.psi.common.kafka;

import com.reactiveminds.psi.SpringContextWrapper;
import com.reactiveminds.psi.common.kafka.pool.KafkaConsumerPool;
import com.reactiveminds.psi.common.kafka.tools.ConversationalClient;
import com.reactiveminds.psi.common.kafka.tools.RequestReplyClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

@Import({KafkaAutoConfiguration.class})
@Configuration
public class KafkaClientsConfiguration {
    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    RequestReplyClient requestReplyClient(){
        return new RequestReplyClient();
    }
    @Bean
    KafkaConsumerPool<byte[], byte[]> consumerPool(){
        return new KafkaConsumerPool<>();
    }
    @Bean
    StringKafkaConsumerPool consumerPoolString(){
        return new StringKafkaConsumerPool();
    }
    @ConditionalOnProperty("spring.kafka.properties.schema.registry.url")
    @Bean
    SpecificAvroSerializer avroSerializer(){
        SpecificAvroSerializer ser = new SpecificAvroSerializer();
        ser.configure(kafkaProperties.buildProducerProperties(), false);
        return ser;
    }
    @ConditionalOnProperty("spring.kafka.properties.schema.registry.url")
    @Bean
    SpecificAvroDeserializer avroDeSerializer(){
        SpecificAvroDeserializer de = new SpecificAvroDeserializer();
        de.configure(kafkaProperties.buildProducerProperties(), false);
        return de;
    }

    @Bean
    public ProducerFactory<byte[],byte[]> bytesKafkaProducerFactory() {
        DefaultKafkaProducerFactory<byte[],byte[]> factory = new DefaultKafkaProducerFactory<>(
                this.kafkaProperties.buildProducerProperties());
        String transactionIdPrefix = this.kafkaProperties.getProducer().getTransactionIdPrefix();
        if (transactionIdPrefix != null) {
            factory.setTransactionIdPrefix(transactionIdPrefix);
        }
        return factory;
    }
    @Bean
    public ProducerFactory<String,String> stringKafkaProducerFactory() {
        DefaultKafkaProducerFactory<String,String> factory = new DefaultKafkaProducerFactory<>(
                this.kafkaProperties.buildProducerProperties());
        String transactionIdPrefix = this.kafkaProperties.getProducer().getTransactionIdPrefix();
        if (transactionIdPrefix != null) {
            factory.setTransactionIdPrefix(transactionIdPrefix);
        }
        factory.setKeySerializer(Serdes.String().serializer());
        factory.setValueSerializer(Serdes.String().serializer());
        return factory;
    }

    @Bean
    StringKafkaTemplate kafkaProducerString(ProducerFactory<String,String> factory){
        return new StringKafkaTemplate(factory);
    }
}
