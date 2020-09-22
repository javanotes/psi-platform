package com.reactiveminds.psi.common.kafka.tools;

import com.reactiveminds.psi.common.util.SerdeUtils;
import com.reactiveminds.psi.common.util.StopWatch;
import com.reactiveminds.psi.common.TwoPCConversation;
import com.reactiveminds.psi.common.err.InternalOperationFailed;
import com.reactiveminds.psi.common.kafka.StringKafkaConsumerPool;
import com.reactiveminds.psi.common.kafka.StringKafkaTemplate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ConversationalClient implements TwoPCConversation {
    private static final Logger log = LoggerFactory.getLogger(ConversationalClient.class);
    @Autowired
    StringKafkaTemplate producer;
    @Autowired
    StringKafkaConsumerPool consumerPool;
    private String topic;
    private int partition;
    private String corrKey;

    @Override
    public long getWriteOffset() {
        return writeOffset;
    }

    private long writeOffset = -1;
    private long readOffset = writeOffset + 1;

    private boolean isStarter = false;
    /**
     * The conversation starter
     * @param topic
     * @param correlationKey
     */
    public ConversationalClient(String topic, String correlationKey){
        this.topic = topic;
        corrKey = correlationKey;
        isStarter = true;
    }
    public ConversationalClient(TopicPartition singlePartition, String correlationKey, long fromOffset){
        topic = singlePartition.topic();
        partition = singlePartition.partition();
        readOffset = fromOffset;
        corrKey = correlationKey;
    }
    @PostConstruct
    void init(){

    }
    @Override
    public int getPartition() {
        return partition;
    }

    @Override
    public long getReadOffset() {
        return readOffset;
    }

    @Override
    public void begin(String hello){
        if(isStarter){
            tell(hello);
        }
    }

    public static final String MASTER = "++m";
    private ListenableFuture<SendResult<String, String>> doSend(String hello){
        ProducerRecord<String, String> rec = new ProducerRecord<>(topic, corrKey, hello);
        if(isStarter){
            rec.headers().add(MASTER, SerdeUtils.stringToBytes("true"));
        }
        return producer.send(rec);
    }
    @Override
    public void tell(String hello){
        try {
            SendResult<String, String> result = doSend(hello).get();
            writeOffset = result.getRecordMetadata().offset();
            readOffset = writeOffset + 1;
            partition = result.getRecordMetadata().partition();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new InternalOperationFailed("unable to publish", e.getCause());
        }
    }
    @Override
    public void send(String hello){
        try {
            doSend(hello);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getTopic() {
        return topic;
    }


    private boolean isValidReply(ConsumerRecord<String, String> rec){
        if(isStarter){
            return rec.headers().lastHeader(MASTER) == null;
        }
        else {
            return rec.headers().lastHeader(MASTER) != null;
        }
    }
    @Override
    public String getCorrKey() {
        return corrKey;
    }

    private String nextReply;
    private boolean foundNextReply(Consumer<String, String> consumer, ConsumerRecords<String, String> records){
        boolean found = false;
        nextReply = null;
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
            for (ConsumerRecord<String, String> record : partitionRecords) {
                if(record.key().equals(corrKey) && isValidReply(record)){
                    nextReply = record.value();
                    found = true;
                }
                readOffset = record.offset() + 1;
                log.debug("#[{}] lastOffset to commit: {}, next read from: {}", corrKey, record.offset(), readOffset);
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(readOffset)));
                if(found)
                    break;
            }
            if(found)
                break;
        }
        return found;
    }


    @Override
    public String listen(long maxAwait, TimeUnit unit) throws TimeoutException {
        Consumer<String, String> consumer = null;
        Exception cause = null;
        try {
            consumer = consumerPool.acquire(5000, TimeUnit.MILLISECONDS, Collections.singletonMap(new TopicPartition(topic, partition), readOffset));
            StopWatch watch = new StopWatch();
            watch.start();
            while(!watch.isExpired(unit.toMillis(maxAwait))){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if(foundNextReply(consumer, records)){
                    return nextReply;
                }

            }

        } catch (Exception e) {
            cause = e;
        }
        finally{
            if(consumer != null){
                consumerPool.release(consumer);
            }
        }
        TimeoutException timeoutException = new TimeoutException();
        if(cause != null)
            timeoutException.initCause(cause);
        throw timeoutException;
    }

    @PreDestroy
    public void close() throws IOException {

    }
}
