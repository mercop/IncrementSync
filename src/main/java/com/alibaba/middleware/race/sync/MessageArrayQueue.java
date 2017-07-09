package com.alibaba.middleware.race.sync;

/**
 * Created by sang on 2017/6/23.
 */
public class MessageArrayQueue {

    private int capacity;

    private int pos;

    private int limit;

    private WrappedMessageArray[] arrays;

    public MessageArrayQueue(int capacity) {
        this.capacity = capacity;
        this.pos = 0;
        this.limit = 0;
        this.arrays = new WrappedMessageArray[this.capacity];
    }

    public void flip(){
        this.limit = pos;
        this.pos = 0;
    }

    public boolean isEmpty(){
        return pos == limit;
    }

    public WrappedMessageArray poll(){
        return arrays[pos ++];
    }

    public void add(WrappedMessageArray wrappedMessageArray){
        arrays[pos ++] = wrappedMessageArray;
    }

    public void clear(){
        arrays = null;
    }

    public boolean isFull(){
        return pos == capacity;
    }
}
