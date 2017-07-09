package com.alibaba.middleware.race.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sang on 2017/6/21.
 */
public class ParseService implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(Server.class);

    private static int LENGTH_OF_MESSAGE_HEADER = "|mysql-bin.000019365|1497265277000|middleware8|student|I|i".getBytes().length;

    private static int LENGTH_OF_INSERT_DATA = "first_name:2:0|NULL|王|last_name:2:0|NULL|名|sex:1:0|NULL|男|score:1:0|NULL|1|score2:1:0|NULL|".getBytes().length;

//    private static int LENGTH_OF_INSERT_DATA = "first_name:2:0|NULL|王|last_name:2:0|NULL|名|sex:1:0|NULL|男|score:1:0|NULL|1|".getBytes().length;

    private static int LENGTH_TO_TYPE = "e5|student|".length();

    private static int LENGTH_OF_NULL = "NULL|".getBytes().length;

    private static int LENGTH_OF_ID_ELEMENT = "|id:1:1|".getBytes().length;

    private static int LENGTH_OF_FIRST_NAME_ELEMENT = "irst_name:2:0|".getBytes().length;

    private static int LENGTH_OF_LAST_NAME_ELEMENT = "ast_name:2:0|".getBytes().length;

    private static int LENGTH_OF_SEX_ELEMENT = "x:2:0|".getBytes().length;

    private static int LENGTH_OF_SCORE_ELEMENT = "1:0|".getBytes().length;

    private static int LENGTH_OF_SCORE_2_ELEMENT = ":1:0|".getBytes().length;


    private static int LENGTH_OF_FIRST_NAME_INSERT = "first_name:2:0|NULL|".length();
    private static int LENGTH_OF_LAST_NAME_INSERT = "last_name:2:0|NULL|".length() + 1;
    private static int LENGTH_OF_SEX_INSERT = "sex:2:0|NULL|".length() + 1;
    private static int LENGTH_OF_SCORE_INSERT = "score:1:0|NULL|".length() + 1;
    private static int LENGTH_OF_SCORE2_INSERT = "score2:1:0|NULL|".length() + 1 ;


    private static byte INSERT = 'I';

    private static byte DELETE = 'D';

    private static byte UPDATE = 'U';

    private static int SIZE_OF_MESSAGE_ARRAY_QUEUE = 1024 << 3;

    private static int INIT_NUM = MessageStore.INIT_NUM - 1;
    private static byte[][] routingArray = MessageStore.routingArray;
    private static int ROW_MASK = MessageStore.ROW_MASK;
    private static int COL = MessageStore.COL;
    private static int SIZE_OF_SQL_DATA_MAPS = MessageStore.MASK_OF_UPDATE_SERVICE;


    private int num;

    private boolean flagOfStart;

    private LinkedBlockingQueue<MessageDataBlock> messageDataBlockQueue;

    private Queue<MessageArrayQueue> currentQueue;

    private MessageArrayQueue messageArrayQueue;

    private boolean flag_1;

    public ParseService(int num, LinkedBlockingQueue<MessageDataBlock> messageDataBlockQueue) {
        this.num = num;
        this.messageDataBlockQueue = messageDataBlockQueue;
        this.flagOfStart = true;
        this.flag_1 = true;
    }

    @Override
    public void run() {
        byte type;
        long oldId = -1;
        long newId = -1;

        boolean isKeySame = true;

        int lengthOfDeletedMessage = 0;
        int lengthOfMessageHeader = LENGTH_OF_MESSAGE_HEADER;
        boolean flagOfDeletedMessage = false;

        //几个byte常量
        byte byte_r = 'r';
        byte byte_sep = '|';
        byte byte_n = '\n';
        byte byte_f = 'f';
        byte byte_l = 'l';
        byte byte_s = 's';
        byte byte_e = 'e';
        byte byte_2 = '2';
        byte byte_i = 'i';
        long column_0 = 0L << 60;
        long column_1 = 1L << 60;
        long column_2 = 2L << 60;
        long column_3 = 3L << 60;
        long column_4 = 4L << 60;

        long first_name = 0L;
        long last_name = 0L;
        long sex = 0L;
        long score = 0L;
        long score_2 = 0L;
        int ii = 0;
        int tmp_1;
        //message 是一个长度为5的数组，从0至4五个数据分别表示五个属性
        //message[0] first_name
        //message[1] last_name
        //message[2] sex
        //message[3] score
        //message[4] score2
        long[] message;
        byte byteValue;
        WrappedMessageArray wrappedMessageArray;

        MessageDataBlock messageDataBlock = null;
        ByteBuffer originalByteBuffer = null;
        UnsafeByteBuffer byteBuffer = null;
        byte[] bytes = new byte[200];
        while (true){
            try {
                messageDataBlock = messageDataBlockQueue.take();
                if (flagOfStart){
                    logger.info(" parseService " + num + "  begins");
                    flagOfStart = false;
                }
                originalByteBuffer = messageDataBlock.getByteBuffer();
                currentQueue = new LinkedList<>();
                messageArrayQueue = new MessageArrayQueue(SIZE_OF_MESSAGE_ARRAY_QUEUE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            byteBuffer = new UnsafeByteBuffer(originalByteBuffer);
            if (byteBuffer.limit()<1){
                break;
            }

            while (true) {

                if (byteBuffer.position() + 2 > byteBuffer.limit()){
                    break;
                }
                tmp_1 = byteBuffer.position();

                if (byteBuffer.get(tmp_1 + lengthOfMessageHeader) != byte_i){
                    for (int i=0; i<20; i++){
                        if (byteBuffer.get(tmp_1 + lengthOfMessageHeader + i) == byte_i){
                            lengthOfMessageHeader += i;
                            break;
                        }
                        if (byteBuffer.get(tmp_1 + lengthOfMessageHeader - i) == byte_i){
                            lengthOfMessageHeader -= i;
                            break;
                        }
                    }
                }
                byteBuffer.position(tmp_1 + lengthOfMessageHeader - 2);


                type = byteBuffer.get();

                byteBuffer.position(byteBuffer.position() + LENGTH_OF_ID_ELEMENT);

                message = null;
                //解析一条增量数据的id
                if (type == UPDATE) {
                    oldId = 0;

                    while (true) {
                        byteValue = byteBuffer.get();
                        if (byteValue != byte_sep) {
                            oldId = ( byteValue & 0x0f) + oldId * 10 ;
                        } else {
                            break;
                        }
                    }

                    newId = 0;
                    while (true) {
                        byteValue = byteBuffer.get();
                        if (byteValue != byte_sep) {
                            newId = ( byteValue & 0x0f) + newId * 10;
                        } else {
                            break;
                        }
                    }


                    isKeySame = (newId == oldId);
                    if (isKeySame){
                        //变更id的时候
                        if (newId > 8000000 || oldId >8000000){
                            while (byteBuffer.position() < byteBuffer.limit() && byteBuffer.get() != '\n');
                            continue;
                        }

                    }

                    if (newId > INIT_NUM && routingArray[(int) (newId >> 8)] == null){
                        int row = (int) (newId >> 8);
                        routingArray[row] = new byte[COL];
                        int i=0;
                        long base = (newId >> 8) << 8;
                        while (i<COL){
                            routingArray[row][i] = (byte) (base & SIZE_OF_SQL_DATA_MAPS);
                            base ++;
                            i ++;
                        }
                    }

                } else if (type == INSERT) {

                    byteBuffer.position(byteBuffer.position() + LENGTH_OF_NULL);
                    newId = 0;
                    while (true) {
                        byteValue = byteBuffer.get();
                        if (byteValue != byte_sep) {
                            newId = ( byteValue & 0x0f) + newId * 10;
                        } else {
                            break;
                        }
                    }
                    oldId = newId;
                    isKeySame = true;

                    if (newId > INIT_NUM && routingArray[(int) (newId >> 8)] == null){
                        int row = (int) (newId >> 8);
                        routingArray[row] = new byte[COL];
                        int i=0;
                        long base = (newId >> 8) << 8;
                        while (i<COL){
                            routingArray[row][i] = (byte) (base & SIZE_OF_SQL_DATA_MAPS);
                            base ++;
                            i++;
                        }
                    }

                    if (byteBuffer.limit() - byteBuffer.position() > 200){
                        int tmp = byteBuffer.position();
                        int i = tmp + LENGTH_OF_INSERT_DATA;
                        byteBuffer.position(i);
                        while (true){
                            if (byteBuffer.get(i) == '\n'){
                                break;
                            }
                            i++;
                        }
                        byteBuffer.position(tmp);

                        byteBuffer.get(bytes, 0, i - tmp + 1);

                        if (newId > 8000000 || oldId >8000000){
                            continue;
                        }
                        message = parseData(bytes);
                    }else {
                        byteBuffer.get(bytes, 0, byteBuffer.limit() - byteBuffer.position());

                        if (newId > 8000000 || oldId >8000000){
                            continue;
                        }

                        message = parseData(bytes);
                    }

                } else if (type == DELETE) {
                    oldId = 0;
                    while (true) {
                        byteValue = byteBuffer.get();
                        if (byteValue != byte_sep) {
                            oldId = ( byteValue & 0x0f) + oldId * 10;
                        } else {
                            break;
                        }
                    }
                    newId = oldId;
                    isKeySame = true;

                    /*************************************************************/

                    if (flagOfDeletedMessage){
                        if (byteBuffer.position() + lengthOfDeletedMessage + 5 <= byteBuffer.limit()){
                            int tmp = byteBuffer.position();
                            if (byteBuffer.get(tmp + lengthOfDeletedMessage) != byte_n){
                                for (int i=0; i<20; i++){
                                    if (byteBuffer.get(tmp + lengthOfDeletedMessage + i) == byte_n){
                                        lengthOfDeletedMessage += i;
                                        break;
                                    }
                                    if (byteBuffer.get(tmp + lengthOfDeletedMessage - i) == byte_n){
                                        lengthOfDeletedMessage -= i;
                                        break;
                                    }

                                }

                            }
                            byteBuffer.position(tmp + lengthOfDeletedMessage + 1);
                        }else {
                            while (byteBuffer.position() < byteBuffer.limit() && byteBuffer.get() != byte_n) ;
                        }

                    }else {
                        while (byteBuffer.get() != byte_n){
                            lengthOfDeletedMessage ++;
                        }
                        flagOfDeletedMessage = true;
                    }

                    if (newId > 8000000 || oldId >8000000){
                        continue;
                    }

                    /*************************************************************/

                } else {
                    if (flag_1){
                        logger.error("fatal error : **** unsupported operation type in parse service ****");
                        flag_1 = false;
                    }
                }


                //解析一条增量数据
                if (type == UPDATE) {

                    first_name = 0L;
                    last_name = 0L;
                    sex = 0L;
                    score = 0L;
                    score_2 = 0L;
                    ii = 0;
                    while (true) {
                        if (byteBuffer.position() >= byteBuffer.limit()){
                            break;
                        }
                        byteValue = byteBuffer.get();
                        if (byteValue == byte_f) {
                            //first_name 更改message[0]里面的数据
                            byteBuffer.position(byteBuffer.position() + LENGTH_OF_FIRST_NAME_ELEMENT);
                            while (byteBuffer.get() != byte_sep) ;

                            while (true) {
                                byteValue = byteBuffer.get();
                                if (byteValue == byte_sep) {
                                    break;
                                }

                                first_name = first_name << 8;
                                first_name = 0xff & byteValue | first_name;
                            }
                            ii++;

                        } else if (byteValue == byte_l) {
                            //last_name 更改message[1]里面的数据
                            //在这里默认的last_name只有两个中文字符，也就是6个byte，一个int是可以存下的，
                            //如果超过两个中文，则会出错，暂不做处理
                            byteBuffer.position(byteBuffer.position() + LENGTH_OF_LAST_NAME_ELEMENT);
                            while (byteBuffer.get() != byte_sep) ;
                            while (true) {
                                byteValue = byteBuffer.get();
                                if (byteValue == byte_sep) {
                                    break;
                                }

                                last_name = last_name << 8;
                                last_name = 0xff & byteValue | last_name;

                            }
                            ii++;
                        } else if (byteValue == byte_s) {
                            byteValue = byteBuffer.get();
                            if (byteValue == byte_e) {
                                //sex 更改message[2]里面的数据
                                byteBuffer.position(byteBuffer.position() + LENGTH_OF_SEX_ELEMENT);
                                while (byteBuffer.get() != byte_sep) ;

                                while (true) {
                                    byteValue = byteBuffer.get();
                                    if (byteValue == byte_sep) {
                                        break;
                                    }

                                    sex = sex << 8;
                                    sex = 0xff & byteValue | sex;
                                }
                                ii++;

                            } else {
                                //判断是score2 还是score
                                byteBuffer.position(byteBuffer.position() + 3);
                                byteValue = byteBuffer.get();
                                if (byteValue == byte_2) {
                                    //score2 更改message[4]里面的数据
                                    byteBuffer.position(byteBuffer.position() + LENGTH_OF_SCORE_2_ELEMENT);
                                    while (byteBuffer.get() != byte_sep) ;

                                    while (true) {
                                        byteValue = byteBuffer.get();
                                        if (byteValue == byte_sep) {
                                            break;
                                        }

                                        score_2 = score_2 << 8;
                                        score_2 = 0xff & byteValue | score_2;
                                    }
                                    ii++;

                                } else {
                                    //score 更改message[3]里面的数据
                                    byteBuffer.position(byteBuffer.position() + LENGTH_OF_SCORE_ELEMENT);
                                    while (byteBuffer.get() != byte_sep) ;

                                    while (true) {
                                        byteValue = byteBuffer.get();
                                        if (byteValue == byte_sep) {
                                            break;
                                        }

                                        score = score << 8;
                                        score = 0xff & byteValue | score;
                                    }
                                    ii++;
                                }
                            }

                        } else if (byteValue == byte_n) {
                            break;
                        }
                    }

                    //通过该方法，使得每次创建的 增量数据数组 的size小于表中列的个数 5
                    //因为很多时候update 增量数据 只是更新了一个属性的数据
                    if (ii>0){

                        message = new long[ii];
                        int j=0;
                        if (first_name != 0L){
                            message[j] = first_name | column_0;
                            j++;
                        }
                        if (last_name != 0L){
                            message[j] = last_name | column_1;
                            j++;
                        }
                        if (sex != 0L){
                            message[j] = sex | column_2;
                            j++;
                        }
                        if (score != 0L){
                            message[j] = score | column_3;
                            j++;
                        }

                        if (score_2 != 0L){
                            message[j] = score_2 | column_4;
                        }
                    }

                }

                /*************************************************************/

                wrappedMessageArray =
                        new WrappedMessageArray(type, isKeySame, newId, oldId, message);
                /*************************************************************/
                messageArrayQueue.add(wrappedMessageArray);

                if (messageArrayQueue.isFull()){
                    messageArrayQueue.flip();
                    currentQueue.add(messageArrayQueue);
                    messageArrayQueue = new MessageArrayQueue(SIZE_OF_MESSAGE_ARRAY_QUEUE);
                }

            }

            //将对应的byteBuffer解析得到的queue放进该解析线程的阻塞队列，
            //供分发线程消费
            messageArrayQueue.flip();
            currentQueue.add(messageArrayQueue);
            messageDataBlock.publishParseMessageQueue(currentQueue);

        }

        //放进去一个空的queue，通过这样的方式通知 分发线程有一个解析服务已经结束了
        messageDataBlock.publishParseMessageQueue(currentQueue);
        logger.info("parse server " + num + " has been over");
    }

    private long[] parseData(byte[] bytes){

        long[] message = new long[5];
        //解析first_name
        int i = LENGTH_OF_FIRST_NAME_INSERT;
        message[0] = 0xff & bytes[i] | message[0];
        i++;

        message[0] = message[0] << 8;
        message[0] = 0xff & bytes[i] | message[0];
        i++;

        message[0] = message[0] << 8;
        message[0] = 0xff & bytes[i] | message[0];
        i++;

        if (bytes[i] != '|'){
            message[0] = message[0] << 8;
            message[0] = 0xff & bytes[i] | message[0];
            i++;

            message[0] = message[0] << 8;
            message[0] = 0xff & bytes[i] | message[0];
            i++;

            message[0] = message[0] << 8;
            message[0] = 0xff & bytes[i] | message[0];
            i++;

        }

        i += LENGTH_OF_LAST_NAME_INSERT;
        message[1] = 0xff & bytes[i] | message[1];
        i++;

        message[1] = message[1] << 8;
        message[1] = 0xff & bytes[i] | message[1];
        i++;

        message[1] = message[1] << 8;
        message[1] = 0xff & bytes[i] | message[1];
        i++;

        if (bytes[i] != '|'){
            message[1] = message[1] << 8;
            message[1] = 0xff & bytes[i] | message[1];
            i++;

            message[1] = message[1] << 8;
            message[1] = 0xff & bytes[i] | message[1];
            i++;

            message[1] = message[1] << 8;
            message[1] = 0xff & bytes[i] | message[1];
            i++;

        }

        i += LENGTH_OF_SEX_INSERT;
        message[2] = 0xff & bytes[i] | message[2];
        i++;

        message[2] = message[2] << 8;
        message[2] = 0xff & bytes[i] | message[2];
        i++;

        message[2] = message[2] << 8;
        message[2] = 0xff & bytes[i] | message[2];
        i++;


        i += LENGTH_OF_SCORE_INSERT;
        while (bytes[i] != '|'){
            message[3] = message[3] << 8;
            message[3] = 0xff & bytes[i] | message[3];
            i++;
        }


        i += LENGTH_OF_SCORE2_INSERT;
        while (bytes[i] != '|'){
            message[4] = message[4] << 8;
            message[4] = 0xff & bytes[i] | message[4];
            i++;
        }

        return message;
    }
}
