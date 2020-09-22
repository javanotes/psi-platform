package com.reactiveminds.psi.common;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class TwoPCConversationClientFactory  {
    public enum Type{TOPIC, RINGBUFF}

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
