package com.reactiveminds.psi.common.imdg;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.reactiveminds.psi.common.AsyncTellFunction;
import com.reactiveminds.psi.common.util.StopWatch;
import com.reactiveminds.psi.common.TwoPCConversation;
import com.reactiveminds.psi.common.err.InternalOperationFailed;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.concurrent.*;

public class GridConversationalClient implements TwoPCConversation, AsyncTellFunction {
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
     * @param correlationKey
     */
    public GridConversationalClient(String correlationKey){
        corrKey = correlationKey;
        isStarter = true;
    }

    /**
     *
     * @param correlationKey
     * @param fromOffset
     */
    public GridConversationalClient(String correlationKey, long fromOffset){
        readOffset = fromOffset;
        corrKey = correlationKey;
    }
    @Override
    public void begin(String hello) {
        if(isStarter){
            tell(hello);
        }
    }

    @Override
    public void tell(String hello) {
        try {
            doSend(hello, false);
        } catch (Exception e) {
            throw new InternalOperationFailed("unable to publish to ringbuff", e);
        }
    }

    private Ringbuffer<Conversation> ringBuffer(){
        return hazelcastInstance.getRingbuffer(getTopic());
    }

    private CompletionStage<Long> doSend(String hello, boolean async) {
        Ringbuffer<Conversation> ringbuffer = ringBuffer();
        Conversation c = new Conversation();
        if(isStarter){
            c.setMaster(true);
        }
        c.setKey(corrKey);
        c.setValue(hello);
        if (!async) {
            writeOffset = ringbuffer.add(c);
            readOffset = writeOffset + 1;
        }
        else{
            return ringbuffer.addAsync(c, OverflowPolicy.OVERWRITE);
        }
        return CompletableFuture.completedStage(writeOffset);
    }

    @Override
    public void send(String hello) {
        doSend(hello, true);
    }

    @Override
    public String getCorrKey() {
        return corrKey;
    }

    private boolean isValidReply(Conversation rec){
        if(isStarter){
            return rec.isMaster() == false;
        }
        else {
            return rec.isMaster() == true;
        }
    }

    private String nextReply;
    private boolean foundNextReply(ReadResultSet<Conversation> resultSet){
        boolean found = false;
        nextReply = null;

        long nextOffset = resultSet.getNextSequenceToReadFrom();
        resultSet.size();
        for (int i=0; i<resultSet.size(); i++){
            Conversation conversation = resultSet.get(i);
            readOffset = resultSet.getSequence(i) + 1;
            if(conversation.getKey().equals(corrKey) && isValidReply(conversation)){
                nextReply = conversation.getValue();
                found = true;
            }
            if(found)
                break;
        }

        return found;
    }

    @Override
    public String listen(long maxAwait, TimeUnit unit) throws TimeoutException {
        {
            Consumer<String, String> consumer = null;
            Throwable cause = null;
            Ringbuffer<Conversation> ringbuffer = ringBuffer();
            try {
                StopWatch watch = new StopWatch();
                watch.start();
                while(!watch.isExpired(unit.toMillis(maxAwait))){
                    boolean found = false;
                    nextReply = null;
                    CompletionStage<ReadResultSet<Conversation>> completionStage = ringbuffer.readManyAsync(readOffset, 0, 50, null);
                    ReadResultSet<Conversation> resultSet = completionStage.toCompletableFuture().get(1000, TimeUnit.MILLISECONDS);
                    if(foundNextReply(resultSet)){
                        return nextReply;
                    }

                }

            } catch (ExecutionException e) {
                cause = e.getCause();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
            TimeoutException timeoutException = new TimeoutException();
            if(cause != null)
                timeoutException.initCause(cause);
            throw timeoutException;
        }
    }
    @Autowired
    private HazelcastInstance hazelcastInstance;
    @PostConstruct
    void init() {
        partition = partition();
        topic = "ringbuff-"+partition;
    }

    private int partition(){
        Assert.notNull(hazelcastInstance, "hazelcast instance not set");
        return hazelcastInstance.getPartitionService().getPartition(corrKey).getPartitionId();
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
    public String getTopic() {
        return topic;
    }

    @Override
    public CompletionStage<Long> apply(String s) {
        return doSend(s, true);
    }
}
