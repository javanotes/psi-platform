package com.reactiveminds.psi.streams.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Deprecated
@ConditionalOnProperty(name = "psi.stream.queryListener.kafkaEndpoint", havingValue = "true")
@Component
class SimpleKafkaService {
    @Autowired
    DataServiceProxy serviceProxy;

    @KafkaListener(topics = "${psi.stream.queryListener.kafkaEndpoint.topics}", concurrency = "${psi.stream.queryListener.kafkaEndpoint.concurrency:1}")
    public Message<byte[]> listen(byte[] storeKey,
                                  @Header(value = KafkaHeaders.REPLY_TOPIC, required = false) byte[] replyTo,
                                  @Header(value = KafkaHeaders.RAW_DATA, required = false) byte[] store,
                                  @Header(value = KafkaHeaders.RECEIVED_MESSAGE_KEY) byte[] key,
                                  @Header(value = KafkaHeaders.CORRELATION_ID) byte[] correlation){

        if(store != null){
            byte[] value = serviceProxy.findInLocal(new String(store, StandardCharsets.UTF_8), storeKey);
            if(value != null){
                return MessageBuilder.withPayload(value)
                        .setHeader(KafkaHeaders.TOPIC, replyTo)
                        .setHeader(KafkaHeaders.MESSAGE_KEY, key)
                        .setHeader(KafkaHeaders.ACKNOWLEDGMENT, 1)
                        .setHeader(KafkaHeaders.CORRELATION_ID, correlation)
                        .build();
            }
        }

        return null;
    }
}
