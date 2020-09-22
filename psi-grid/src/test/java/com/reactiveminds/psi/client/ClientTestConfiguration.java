package com.reactiveminds.psi.client;

import com.reactiveminds.psi.common.imdg.ClientConfiguration;
import com.reactiveminds.psi.server.loaders.LoaderConfiguration;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

@EnableKafka
@Import({ClientConfiguration.class, KafkaAutoConfiguration.class, LoaderConfiguration.class})
@TestConfiguration
class ClientTestConfiguration {
    @Bean
    public ConcurrentKafkaListenerContainerFactory<SpecificRecord, SpecificRecord>
    kafkaListenerContainerFactory(ConsumerFactory<SpecificRecord, SpecificRecord> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<SpecificRecord, SpecificRecord> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

}
