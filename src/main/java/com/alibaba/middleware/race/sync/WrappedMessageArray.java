package com.alibaba.middleware.race.sync;

/**
 * Created by sang on 2017/6/15.
 */
public class WrappedMessageArray {

    public WrappedMessageArray(byte operationType,
                               boolean isKeySame,
                               long newKeyValue,
                               long oldKeyValue,
                               long[] messageArray) {
        this.operationType = operationType;
        this.isKeySame = isKeySame;
        this.newKeyValue = newKeyValue;
        this.oldKeyValue = oldKeyValue;

        this.messageArray = messageArray;
    }

    byte operationType;

    boolean isKeySame;

    long newKeyValue;

    long oldKeyValue;

    long[] messageArray;

}
