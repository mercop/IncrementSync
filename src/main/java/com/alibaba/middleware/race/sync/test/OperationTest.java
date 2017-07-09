package com.alibaba.middleware.race.sync.test;

/**
 * Created by sang on 2017/6/23.
 */
public class OperationTest {

    public static int ROW = 1 << 24;
    public static int COL = 1 << 8;

    public static int ROW_MASK = COL -1;

    public static int INIT_ROW = 1 << 15;
    public static int INIT_NUM = 1 << 23;


    public static void main(String[] args){
//        long start = System.currentTimeMillis();
//        byte[][] routingArray = new byte[ROW][];
//        for (int i=0; i<INIT_ROW; i++){
//            routingArray[i] = new byte[COL];
//        }
//
//        for (int i=0; i<INIT_NUM; i++){
//            routingArray[i >> 8][i & ROW_MASK] = (byte) (i & ROW_MASK);
//        }
//
//        long end = System.currentTimeMillis();
//        System.out.println(end - start);
        long a = 257;
        long b = (a >> 8) <<8;
        System.out.println("a "+a +"  b  "+b);

    }


}
