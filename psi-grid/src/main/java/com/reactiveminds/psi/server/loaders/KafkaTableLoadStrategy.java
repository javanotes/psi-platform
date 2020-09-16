package com.reactiveminds.psi.server.loaders;


import com.reactiveminds.psi.server.KeyLoadStrategy;
import com.reactiveminds.psi.common.err.GridOperationFailedException;
import com.reactiveminds.psi.common.kafka.pool.BytesMessagePayload;
import com.reactiveminds.psi.common.kafka.pool.MessagePayload;
import com.reactiveminds.psi.common.kafka.tools.RequestReplyClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.Base64;
import java.util.Properties;
import java.util.UUID;

class KafkaTableLoadStrategy implements KeyLoadStrategy<SpecificRecord, SpecificRecord>,RawKeyLoadStrategy {
    private String psiStreamEndpoints;

    private static final Logger log = LoggerFactory.getLogger(KafkaTableLoadStrategy.class);
    @Autowired
    SpecificAvroDeserializer deserializer;
    @Autowired
    SpecificAvroSerializer serializer;
    @Override
    public SpecificRecord findByKey(SpecificRecord key) throws Exception {
        byte[] val = findByKey(serializer.serialize(null, key));
        return val != null ? deserializer.deserialize(null, val) : null;
    }

    private String kStore;
    private String reqrepTopic;
    private String reqUri;

    @Override
    public void initialize(Properties props) {
        kStore = props.getProperty("map");
        reqrepTopic = props.getProperty("key.load.strategy.ktable.request-reply-topic");
        reqUri = props.getProperty("key.load.strategy.ktable.request-uri");
    }

    @Autowired
    RequestReplyClient requestReplyClient;
    @Override
    public byte[] findByKey(byte[] key)  {
        if(reqUri != null){
            return usingHttp(key);
        }
        else if(reqrepTopic != null){
            return usingKafka(key);
        }
        else
            throw new GridOperationFailedException("Either request-reply-topic or request-uri to be set for ktable map store loading! ");
    }

    private byte[] usingKafka(byte[] key){
        BytesMessagePayload messagePayload = new BytesMessagePayload(key);
        messagePayload.setKafkaStore(kStore);
        messagePayload.setCorrelationId(UUID.randomUUID().toString());
        messagePayload.setPayload(key);
        messagePayload.setRequestreplyTopic(reqrepTopic);
        messagePayload.setType(MessagePayload.MessageType.REQUEST);
        return requestReplyClient.sendAndGet(messagePayload, Duration.ofSeconds(120));
    }
    private byte[] usingHttp(byte[] key){
        //TODO: load balance
        String uri = reqUri + kStore + "/" + Base64.getEncoder().encodeToString(key);
        log.debug("Invoke GET {}",uri);
        SimpleHttpClient httpClient = new SimpleHttpClient(uri);
        httpClient.run();
        String content = httpClient.getResponseContent();
        return content != null ? Base64.getDecoder().decode(content) : null;
    }

}
