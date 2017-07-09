package com.alibaba.middleware.race.sync;

import com.lmax.disruptor.EventFactory;

import java.util.Queue;

/**
 * Created by sang on 2017/6/25.
 */
public class ValueEvent {

    public final static EventFactory<ValueEvent> EVENT_FACTORY = new EventFactory<ValueEvent>() {
        public ValueEvent newInstance() {
            return new ValueEvent();
        }
    };

    private Queue<MessageArrayQueue> messageArrayQueues;

    public Queue<MessageArrayQueue> getMessageArrayQueues() {
        return messageArrayQueues;
    }

    public void setMessageArrayQueues(Queue<MessageArrayQueue> messageArrayQueues) {
        this.messageArrayQueues = messageArrayQueues;
    }
}
