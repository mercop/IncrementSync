package com.alibaba.middleware.race.sync;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 * Created by sang on 2017/6/29.
 */
public class UnsafeByteBuffer {

    public static Unsafe UNSAFE;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private int position;

    private int limit;

    private ByteBuffer byteBuffer;

    private long address;

    public UnsafeByteBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
        this.address = ((DirectBuffer)byteBuffer).address();
        this.position = 0;
        this.limit = byteBuffer.limit();
    }

    public void position(int position){
        this.position = position;
    }
    public int position(){
        return this.position;
    }
    public byte get(){
        return UNSAFE.getByte(address + (position ++));
    }

    public byte get(int position){
        return UNSAFE.getByte(address + position);
    }

    public void get(byte[] bytes, int offset, int length){
        for (int i = 0; i<length; i++){
            bytes[offset + i] = UNSAFE.getByte(address + (position ++));
        }
    }

    public void flip(){
        this.limit = position;
        this.position = 0;
    }

    public int limit(){
        return limit;
    }
}
