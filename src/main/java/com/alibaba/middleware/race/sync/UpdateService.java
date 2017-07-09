package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sang on 2017/6/21.
 */
public class UpdateService implements Runnable{

    private static Logger logger = LoggerFactory.getLogger(Server.class);

    private static byte INSERT = 'I';

    private static byte DELETE = 'D';

    private static byte UPDATE = 'U';

    byte byte_r = 'r';
    byte byte_sep = '|';
    byte byte_n = '\n';
    byte byte_f = 'f';
    byte byte_l = 'l';
    byte byte_s = 's';
    byte byte_e = 'e';
    byte byte_2 = '2';
    byte byte_i = 'i';

    private static int LENGTH_OF_FIRST_NAME_INSERT = "first_name:2:0|NULL|".length();
    private static int LENGTH_OF_LAST_NAME_INSERT = "last_name:2:0|NULL|".length()+1;
    private static int LENGTH_OF_SEX_INSERT = "sex:2:0|NULL|".length()+1;
    private static int LENGTH_OF_SCORE_INSERT = "score:1:0|NULL|".length()+1;
    private static int LENGTH_OF_SCORE2_INSERT = "score2:1:0|NULL|".length()+1;


    private static int LENGTH_OF_NULL = "NULL|".getBytes().length;

    private static int LENGTH_OF_ID_ELEMENT = "|id:1:1|".getBytes().length;

    private static int LENGTH_OF_FIRST_NAME_ELEMENT = "irst_name:2:0|".getBytes().length;

    private static int LENGTH_OF_LAST_NAME_ELEMENT = "ast_name:2:0|".getBytes().length;

    private static int LENGTH_OF_SEX_ELEMENT = "x:2:0|".getBytes().length;

    private static int LENGTH_OF_SCORE_ELEMENT = "1:0|".getBytes().length;

    private static int LENGTH_OF_SCORE_2_ELEMENT = ":1:0|".getBytes().length;

    private long SIZE_OF_SQL_DATA_ARRAY_LONG;

    private int MASK_OF_UPDATE_SERVICE;

    private int num;

    private long start;

    private long end;

    private CountDownLatch countDownLatchOfUpdateServices;

    private long[][] sqlDataArray;

    private ConcurrentHashMap<Long, long[]> sqlDataMap;

    private LinkedBlockingQueue<MessageArrayQueue> updateMessageQueues;

    private Map<Long, long[]> sqlDataMapOfThread;

    private boolean flagOfStart;

    private boolean flag_1;

    public UpdateService(long SIZE_OF_SQL_DATA_ARRAY_LONG, int MASK_OF_UPDATE_SERVICE, int num, long start, long end,
                         CountDownLatch countDownLatchOfUpdateServices,
                         long[][] sqlDataArray, ConcurrentHashMap<Long, long[]> sqlDataMap,
                         LinkedBlockingQueue<MessageArrayQueue> updateMessageQueues) {
        this.SIZE_OF_SQL_DATA_ARRAY_LONG = SIZE_OF_SQL_DATA_ARRAY_LONG;
        this.MASK_OF_UPDATE_SERVICE = MASK_OF_UPDATE_SERVICE;
        this.num = num;
        this.start = start;
        this.end = end;
        this.countDownLatchOfUpdateServices = countDownLatchOfUpdateServices;
        this.sqlDataArray = sqlDataArray;
        this.sqlDataMap = sqlDataMap;

        this.updateMessageQueues = updateMessageQueues;

        this.sqlDataMapOfThread = new HashMap<>();

        this.flagOfStart = true;
        this.flag_1 = true;
    }

    @Override
    public void run() {
        MessageArrayQueue queue = null;
        WrappedMessageArray wrappedMessageArray;

        //几个long常量
        long oldId;
        long newId;
        int part;
        long[] sqlData;
        long[] sqlDataOfUpdate;
        long column;
        long mask = 0xFFFFFFFFFFFFFFL;
        while (true){
            try {
                queue = updateMessageQueues.take();
                if (flagOfStart){
                    logger.info(" updateService " + num + " begins ");
                    flagOfStart = false;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (queue.isEmpty()){
                break;
            }

            while (!queue.isEmpty()){
                wrappedMessageArray = queue.poll();

                if (wrappedMessageArray.operationType == UPDATE){

                    if (wrappedMessageArray.isKeySame){
                        //键值没有发生改变

                        //确定旧的键值对应的数据对象的位置
                        oldId = wrappedMessageArray.oldKeyValue;
                        part = (int)(oldId & MASK_OF_UPDATE_SERVICE);

                        if (part == num){
                            //不应该属于线程的私有变量

                            if (oldId < SIZE_OF_SQL_DATA_ARRAY_LONG){
                                sqlData = sqlDataArray[(int)oldId];

                            }else {
                                sqlData = sqlDataMap.get(oldId);
                            }


                        }else {
                            //属于线程的私有变量
                            sqlData = sqlDataMapOfThread.get(oldId);
                        }
                        /************************************************************/
                        //本地测试数据里面有一部分脏数据，在线上测试环境可以把这部分拿掉
                        if (sqlData == null){
                            if (flag_1){
                                logger.error("fatal error : sqlData == null");
                                flag_1 = false;
                            }
                            continue;
                        }

                        if (sqlData != null){
                            //更新数据
                            sqlDataOfUpdate = wrappedMessageArray.messageArray;
                            for (int i=0; i<sqlDataOfUpdate.length; i++){
                                column = sqlDataOfUpdate[i] >>> 60;
                                sqlData[(int) column] = sqlDataOfUpdate[i] & mask;
                            }
                        }
                        /************************************************************/

                    }else {

                        //键值发生改变

                        //确定旧的键值对应的数据对象的位置，并删除
                        oldId = wrappedMessageArray.oldKeyValue;
                        part = (int) (oldId & MASK_OF_UPDATE_SERVICE);
                        if (part == num) {
                            //不应该属于线程的私有变量

                            if (oldId < SIZE_OF_SQL_DATA_ARRAY_LONG) {
                                sqlData = sqlDataArray[(int) oldId];
                                sqlDataArray[(int) oldId] = null;


                            } else {
                                sqlData = sqlDataMap.get(oldId);
                                sqlDataMap.remove(oldId);
                            }

                        } else {
                            //属于线程的私有变量

                            sqlData = sqlDataMapOfThread.get(oldId);
                            sqlDataMapOfThread.remove(oldId);
                        }

                        /************************************************************/
                        //本地测试数据里面有一部分脏数据，在线上测试环境可以把这部分拿掉
                        if (sqlData == null) {
                            continue;

                        }

                        //线上环境不需要再进行这样的判断

                        if (sqlData != null){

                            //更新数据
                            sqlDataOfUpdate = wrappedMessageArray.messageArray;
                            //更改键值的增量数据中，很有可能并没有更改其他字段
                            if (sqlDataOfUpdate != null){

                                for (int i=0; i<sqlDataOfUpdate.length; i++){
                                    column = sqlDataOfUpdate[i] >>> 60;
                                    sqlData[(int) column] = sqlDataOfUpdate[i] & mask;
                                }
                            }
                        }

                        /************************************************************/

                        //确定新的键值的数据对象应该在的位置，并赋值
                        newId = wrappedMessageArray.newKeyValue;
                        part = (int) (newId & MASK_OF_UPDATE_SERVICE);

                        if (part == num) {
                            //不应该属于线程的私有变量

                            if (newId < SIZE_OF_SQL_DATA_ARRAY_LONG) {
                                sqlDataArray[(int) newId] = sqlData;
                            } else {
                                sqlDataMap.put(newId, sqlData);
                            }
                        } else {
                            //属于线程的私有变量

                            sqlDataMapOfThread.put(newId, sqlData);
                        }
                    }

                }else if (wrappedMessageArray.operationType == INSERT){
                    newId = wrappedMessageArray.newKeyValue;

                    part = (int) (newId & MASK_OF_UPDATE_SERVICE);
//                    long[] sqlData_1 = parseData(wrappedMessageArray.bytes);

                    if (part == num){
                        //不应该属于线程的私有变量
                        if (newId < SIZE_OF_SQL_DATA_ARRAY_LONG){
                            sqlDataArray[(int)newId] = wrappedMessageArray.messageArray;
                        }else {
                            sqlDataMap.put(newId, wrappedMessageArray.messageArray);
                        }
                    }else {
                        //属于线程的私有变量
                        sqlDataMapOfThread.put(newId, wrappedMessageArray.messageArray);
                    }


                }else if (wrappedMessageArray.operationType == DELETE){

                    oldId = wrappedMessageArray.oldKeyValue;

                    part = (int) (oldId & MASK_OF_UPDATE_SERVICE);

                    if (part == num){
                        //不应该属于线程的私有变量

                        if (oldId < SIZE_OF_SQL_DATA_ARRAY_LONG){
                           sqlDataArray[(int)oldId] = null;
                        }else {
                            sqlDataMap.remove(oldId);
                        }
                    }else {
                        //属于线程的私有变量
                        sqlDataMapOfThread.remove(oldId);
                    }

                }else {
                    logger.error("fatal error : unsupported operation type");
                }
                wrappedMessageArray.messageArray = null;
                wrappedMessageArray = null;
            }
            queue.clear();
        }

        /********************************************************/
        //这里的更新数据可以只更新校验范围内的数据
        //将校验范围内的数据更新到数组中
        for (Long keyValue : sqlDataMapOfThread.keySet()){
            if (keyValue > start && keyValue < end){
                sqlDataArray[keyValue.intValue()] = sqlDataMapOfThread.get(keyValue);
            }
        }
        this.countDownLatchOfUpdateServices.countDown();
        logger.info("update service" + num + " has been over");
    }

}
