package com.reactiveminds.psi.common.kafka;

import com.reactiveminds.psi.common.kafka.pool.KafkaConsumerPool;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.annotation.PostConstruct;

public class StringKafkaConsumerPool extends KafkaConsumerPool<String, String> {
    public StringKafkaConsumerPool() {
        super();

    }
    @PostConstruct
    void init(){
        setConsumerProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        setConsumerProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        setConsumerProperty("enable.auto.commit", false);
    }
}
