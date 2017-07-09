package com.alibaba.middleware.race.sync;

import java.nio.ByteBuffer;

/**
 * Created by sang on 2017/6/25.
 */
public class SimpleByteBuffer {

    private byte[] byteArray;

    private int position;

    private int limit;

    private int capacity;

    public SimpleByteBuffer(ByteBuffer byteBuffer) {
        this.byteArray = byteBuffer.array();
        this.position = 0;
        this.limit = byteBuffer.limit();
        this.capacity = byteBuffer.capacity();

    }

    public byte get(int position){
        return this.byteArray[position];
    }

    public byte get(){
        return this.byteArray[position++];
    }

    public int position(){
        return this.position;
    }

    public void position(int position){
        this.position = position;
    }

    public int limit(){
        return this.limit;
    }
}
