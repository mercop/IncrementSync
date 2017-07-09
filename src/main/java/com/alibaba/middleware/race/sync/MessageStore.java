package com.alibaba.middleware.race.sync;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.*;

/**
 * Created by sang on 2017/6/21.
 */
public class MessageStore {

    private static Logger logger = LoggerFactory.getLogger(Server.class);

    private int start;

    private int end;

    //更新线程数一定要是2的整数倍
    private static int NUM_OF_UPDATE_SERVICE = 8;

    public static int MASK_OF_UPDATE_SERVICE = NUM_OF_UPDATE_SERVICE - 1;

    //解析线程数
    private static int NUM_OF_PARSE_SERVICE = 8;

    private static int SIZE_OF_DATA_BLOCK_QUEUE = NUM_OF_PARSE_SERVICE;

    //mappedByteBuffer的容量
    private static int SIZE_OF_BYTE_BUFFER = 1 * 1024 * 1024;


    private static int SIZE_OF_RING_BUFFER = 32;


    private static RingBuffer<ValueEvent> ringBuffer;

    public static LinkedBlockingQueue<MessageDataBlock> messageDataBlockQueue =
            new LinkedBlockingQueue<>(SIZE_OF_DATA_BLOCK_QUEUE);

    //分发线程使用到的routing 数据
    public static int ROW = 1 << 24;
    public static int COL = 1 << 8;

    public static int ROW_MASK = COL -1;

    public static int INIT_ROW = 1 << 15;
    public static int INIT_NUM = 1 << 23;

    public static byte[][] routingArray = new byte[ROW][];
    static {
        for (int i=0; i<INIT_ROW; i++){
            routingArray[i] = new byte[COL];
        }

        for (int i=0; i<INIT_NUM; i++){
            routingArray[i >> 8][i & ROW_MASK] = (byte) (i & MASK_OF_UPDATE_SERVICE);
        }

    }

    //更新线程的增量数据队列
    private static int SIZE_OF_UPDATE_MESSAGE_QUEUE = 512;
    private static LinkedBlockingQueue<MessageArrayQueue>[] updateMessageQueues =
            new LinkedBlockingQueue[NUM_OF_UPDATE_SERVICE];
    static {
        for (int i=0; i<NUM_OF_UPDATE_SERVICE; i++){
            updateMessageQueues[i] = new LinkedBlockingQueue<>(SIZE_OF_UPDATE_MESSAGE_QUEUE);
        }
    }

    private static int SIZE_OF_QUEUE_DISPATCHED_TO_UPDATE_SERVICE = 1024;
    private static MessageArrayQueue[] wrappedMessageArrayQueues = new MessageArrayQueue[NUM_OF_UPDATE_SERVICE];
    static {
        for (int i=0; i<NUM_OF_UPDATE_SERVICE; i++){
            wrappedMessageArrayQueues[i] = new MessageArrayQueue(SIZE_OF_QUEUE_DISPATCHED_TO_UPDATE_SERVICE);
        }
    }


    //存储sqlData对象
    private static int SIZE_OF_SQL_DATA_ARRAY = 10000000;
    private static long SIZE_OF_SQL_DATA_ARRAY_LONG = SIZE_OF_SQL_DATA_ARRAY;
    private static long[][] sqlDataArray = new long[SIZE_OF_SQL_DATA_ARRAY][];
    private static ConcurrentHashMap<Long, long[]>[] sqlDataMaps = new ConcurrentHashMap[NUM_OF_UPDATE_SERVICE];
    static {
        for (int i=0; i<NUM_OF_UPDATE_SERVICE; i++){
            sqlDataMaps[i] = new ConcurrentHashMap<>();
        }
    }

    public volatile Boolean isOver;

    private Thread readThread;

    private Thread[] parseThreads;

    private Thread[] updateThreads;

    private static int countOfCompletedParseService = 0;


    private CountDownLatch countDownLatchOfUpdateServices;

    private static MessageStore INSTANCE = new MessageStore();

    private MessageStore() {
    }

    //启动数据读、解析、分发、更新服务
    public void startSync(){

        //初始化disruptor
        ExecutorService exec = Executors.newCachedThreadPool();
        Disruptor<ValueEvent> disruptor = new Disruptor<>(ValueEvent.EVENT_FACTORY, SIZE_OF_RING_BUFFER, exec);

        final EventHandler<ValueEvent> handler = new EventHandler<ValueEvent>() {
            public void onEvent(final ValueEvent event, final long sequence, final boolean endOfBatch) throws Exception {
                dispatch(event.getMessageArrayQueues());
            }
        };

        disruptor.handleEventsWith(handler);
        ringBuffer = disruptor.start();

        //初始化读、解析、更新线程
        readThread = new Thread(new ReadService( NUM_OF_PARSE_SERVICE, SIZE_OF_BYTE_BUFFER, ringBuffer, messageDataBlockQueue));
        readThread.setName("readService");

        parseThreads = new Thread[NUM_OF_PARSE_SERVICE];
        for (int i=0; i<NUM_OF_PARSE_SERVICE; i++){
            parseThreads[i] = new Thread(new ParseService(i, messageDataBlockQueue));
            parseThreads[i].setName("parseService_" + i);
        }

        this.isOver = false;

        this.countDownLatchOfUpdateServices = new CountDownLatch(NUM_OF_UPDATE_SERVICE);
        this.updateThreads = new Thread[NUM_OF_UPDATE_SERVICE];
        for (int i=0; i < NUM_OF_UPDATE_SERVICE; i++){
            updateThreads[i] = new Thread(new UpdateService(SIZE_OF_SQL_DATA_ARRAY_LONG, MASK_OF_UPDATE_SERVICE, i,
                    start, end, countDownLatchOfUpdateServices, sqlDataArray, sqlDataMaps[i], updateMessageQueues[i]));
            updateThreads[i].setName("updateService_" + i);
        }

        //启动读、解析、更新线程
        readThread.start();
        for (int i=0; i<NUM_OF_PARSE_SERVICE; i++){
            parseThreads[i].start();
        }
        for (int i=0; i<NUM_OF_UPDATE_SERVICE; i++){
            updateThreads[i].start();
        }

    }

    public void setCheckScope(int start, int end){
        this.start = start;
        this.end = end;
    }


    private void dispatch(Queue<MessageArrayQueue> queue){
        MessageArrayQueue messageArrayQueue;
        WrappedMessageArray wrappedMessageArray;
        int part;

        //判断所有的解析任务是否都已经完成
        if (queue.isEmpty()){
            countOfCompletedParseService ++;
            if (countOfCompletedParseService >= NUM_OF_PARSE_SERVICE){

                //分发最后未满的消息队列
                for (int i=0; i<NUM_OF_UPDATE_SERVICE; i++){
                    try {
                        wrappedMessageArrayQueues[i].flip();
                        updateMessageQueues[i].put(wrappedMessageArrayQueues[i]);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }


                //分发任务结束，向每个更新线程添加一个空的queue
                for (int i=0; i<NUM_OF_UPDATE_SERVICE; i++){
                    try {
                        updateMessageQueues[i].put(new MessageArrayQueue(0));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                logger.info("all message has been dispatched, waiting for the update service to be over");

                //等待所有的更新服务全部完成
                try {
                    countDownLatchOfUpdateServices.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                isOver = true;
                return;
            }
        }

        while (!queue.isEmpty()){
            messageArrayQueue = queue.poll();

            while (!messageArrayQueue.isEmpty()){

                wrappedMessageArray = messageArrayQueue.poll();

                if (wrappedMessageArray.isKeySame){
                    part = routingArray[(int) (wrappedMessageArray.oldKeyValue >> 8)][(int) (wrappedMessageArray.oldKeyValue & ROW_MASK)];
                }else {
                    part = routingArray[(int) (wrappedMessageArray.oldKeyValue >> 8)][(int) (wrappedMessageArray.oldKeyValue & ROW_MASK)];

                    routingArray[(int) (wrappedMessageArray.newKeyValue >> 8)][(int) (wrappedMessageArray.newKeyValue & ROW_MASK)] = (byte) part;
                }

                wrappedMessageArrayQueues[part].add(wrappedMessageArray);
                if (wrappedMessageArrayQueues[part].isFull()){
                    try {
                        wrappedMessageArrayQueues[part].flip();

                        updateMessageQueues[part].put(wrappedMessageArrayQueues[part]);

                        wrappedMessageArrayQueues[part] = new MessageArrayQueue(SIZE_OF_QUEUE_DISPATCHED_TO_UPDATE_SERVICE);

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            messageArrayQueue.clear();
        }
    }


    public static MessageStore getInstance(){
        return INSTANCE;
    }

    public static long[][] getSqlDataArray() {
        return sqlDataArray;
    }
}
