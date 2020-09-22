package com.reactiveminds.psi.common;

import com.reactiveminds.psi.common.imdg.GridConversationalClient;
import com.reactiveminds.psi.common.kafka.tools.ConversationalClient;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.util.function.Function;
import java.util.function.Supplier;

@Configuration
public class TwoPCConversationClientFactory  {
    public enum Type{TOPIC, RINGBUFF}

    /**
     *
     * @param topic
     * @param txnId
     * @return
     */
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    @Bean
    ConversationalClient conversationLeader(String topic, String txnId){
        return new ConversationalClient(topic, txnId);
    }

    /**
     *
     * @param topic
     * @param singlePartition
     * @param txnId
     * @param fromOffset
     * @return
     */
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    @Bean
    ConversationalClient conversationFollower(String topic, String txnId, int singlePartition, long fromOffset){
        return new ConversationalClient(new TopicPartition(topic, singlePartition), txnId, fromOffset);
    }
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    @Bean
    GridConversationalClient conversationLeader2(String txnId){
        return new GridConversationalClient(txnId);
    }

    /**
     *
     * @param txnId
     * @param fromOffset
     * @return
     */
    @Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    @Bean
    GridConversationalClient conversationFollower2(String txnId, long fromOffset){
        return new GridConversationalClient(txnId, fromOffset);
    }
    @Autowired
    BeanFactory beanFactory;
    public TwoPCConversation getLeader(Type type, String topic, String txnId) {
        switch (type){

            case TOPIC:
                return (TwoPCConversation) beanFactory.getBean("conversationLeader", topic, txnId);
            case RINGBUFF:
                return (TwoPCConversation) beanFactory.getBean("conversationLeader2", txnId);
        }
        return null;
    }
    public TwoPCConversation getFollower(Type type, String topic, String txnId, int singlePartition, long fromOffset) {
        switch (type){

            case TOPIC:
                return (TwoPCConversation) beanFactory.getBean("conversationFollower", topic, txnId, singlePartition, fromOffset);
            case RINGBUFF:
                return (TwoPCConversation) beanFactory.getBean("conversationFollower2", txnId, fromOffset);
        }
        return null;
    }
}
