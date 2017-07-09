package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sang on 2017/6/22.
 */
public class Utils {

    private static Logger logger = LoggerFactory.getLogger(Server.class);

    private static long BYTE = (1<<8) -1;

    private static long LONG_BYTE =  0X0030;

    private StringBuilder sb;

    public Utils() {
        this.sb = new StringBuilder();
    }

    public byte[] longToBytes(long value){
        long tmp = value;
        int len =0;
        while ( tmp >0 ){
            len ++;
            tmp = tmp >> 8;
        }
        byte[] bytes = new byte[len];
        tmp = value;
        for (int i=len-1; i>=0; i--){
            bytes[i] = (byte) (tmp & BYTE);
            tmp = tmp >> 8;
        }
        return bytes;
    }

    //默认value的数值在百万的区间
    public byte[] longValueToBytes(long value){
        byte[] bytes = new byte[7];
        bytes[6] = (byte) (value % 10 | LONG_BYTE);

        value /= 10;
        bytes[5] = (byte) (value % 10 | LONG_BYTE);

        value /= 10;
        bytes[4] = (byte) (value % 10 | LONG_BYTE);

        value /= 10;
        bytes[3] = (byte) (value % 10 | LONG_BYTE);

        value /= 10;
        bytes[2] = (byte) (value % 10 | LONG_BYTE);

        value /= 10;
        bytes[1] = (byte) (value % 10 | LONG_BYTE);

        value /= 10;
        bytes[0] = (byte) (value % 10 | LONG_BYTE);

        return bytes;
    }

    public String getMessageFromIdAndBytes(long id, long[] message){
        sb.setLength(0);
        sb.append(id);
        if (message != null){
            for (int i=0; i<message.length; i++){
                if (message[i]>0){
                    sb.append('\t').append(new String(longToBytes(message[i])));
                }
            }
            sb.append('\n');
        }

        return sb.toString();
    }

    public static void main(String[] args){
        Utils utils = new Utils();
        byte[] bytes = utils.longValueToBytes(1000000);
        String str = new String(bytes);
        int a=0;
    }
}
