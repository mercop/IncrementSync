package com.alibaba.middleware.race.sync;

import com.lmax.disruptor.RingBuffer;

import java.nio.ByteBuffer;
import java.util.Queue;

/**
 * Created by sang on 2017/6/25.
 */
public class MessageDataBlock {

    private RingBuffer<ValueEvent> ringBuffer;

    private long seq;

    private ByteBuffer byteBuffer;

    private ValueEvent valueEvent;

    public MessageDataBlock(RingBuffer<ValueEvent> ringBuffer, long seq, ValueEvent valueEvent, ByteBuffer byteBuffer) {
        this.ringBuffer = ringBuffer;
        this.seq = seq;
        this.byteBuffer = byteBuffer;
        this.valueEvent = valueEvent;
    }

    public void publishParseMessageQueue(Queue<MessageArrayQueue> messageArrayQueues){
        this.valueEvent.setMessageArrayQueues(messageArrayQueues);
        this.ringBuffer.publish(seq);
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }
}
