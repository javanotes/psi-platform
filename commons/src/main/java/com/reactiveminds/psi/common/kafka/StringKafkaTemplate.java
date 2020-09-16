package com.reactiveminds.psi.common.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

public class StringKafkaTemplate extends KafkaTemplate<String, String> {
    public StringKafkaTemplate(ProducerFactory<String, String> producerFactory) {
        super(producerFactory);
    }
}
