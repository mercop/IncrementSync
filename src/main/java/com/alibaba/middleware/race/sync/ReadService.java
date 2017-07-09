package com.alibaba.middleware.race.sync;

import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sang on 2017/6/15.
 */
public class ReadService implements Runnable{

    private static Logger logger = LoggerFactory.getLogger(Server.class);

    private int NUM_OF_PARSE_SERVICE;

    private int SIZE_OF_BYTE_BUFFER;

    private int size;

    //为了实现解析数据数据分发消息的时候采用 多生产者 单消费者 设计模式
    //所以读数据的数据队列的时候，加入相关的ringBuffer的的信息，从而使得分发消息的时候能够保证有序性
    private RingBuffer<ValueEvent> ringBuffer;

    private LinkedBlockingQueue<MessageDataBlock> messageDataBlockQueue;


    //内部变量
    private String[] dataFileArray;

    private int dataFileIndex;

    private FileChannel fileChannel;

    private long fileSize;

    private long readPosition;

    private boolean overOfCurrentDataFile;

    private boolean flagOfStart;

    public ReadService(int NUM_OF_PARSE_SERVICE, int SIZE_OF_BYTE_BUFFER,
                       RingBuffer<ValueEvent> ringBuffer,
                       LinkedBlockingQueue<MessageDataBlock> messageDataBlockQueue) {
        this.NUM_OF_PARSE_SERVICE = NUM_OF_PARSE_SERVICE;
        this.SIZE_OF_BYTE_BUFFER = SIZE_OF_BYTE_BUFFER;

        this.ringBuffer = ringBuffer;
        this.messageDataBlockQueue = messageDataBlockQueue;
        this.flagOfStart = true;
        this.size = 1024 * 64;
        initFileNameArray();
    }

    private void initFileNameArray(){
        String filePath = Constants.DATA_HOME;
        File directory = new File(filePath);
        if (!directory.isDirectory()){
            logger.error("fatal error : " + filePath + " is not a valid path of data directory");
            return;
        }
        dataFileArray = new String[10];
        for (int i=0; i < dataFileArray.length; i++){
            dataFileArray[i] = filePath + "/" + (i+1) + ".txt";
            logger.info(dataFileArray[i]);
        }
        dataFileIndex = 0;

    }

    private void init(){
        try {
            BufferedReader bufferedReader = new BufferedReader( new InputStreamReader(new FileInputStream(dataFileArray[0])));
            String line = bufferedReader.readLine();
            bufferedReader.close();
            logger.info("first line : " + line);

            logger.info("begin to read data file " + dataFileArray[dataFileIndex]);
            this.fileChannel = new RandomAccessFile(dataFileArray[dataFileIndex], "r").getChannel();
            this.fileSize = fileChannel.size();
            this.readPosition = 0;
            this.overOfCurrentDataFile = false;

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    @Override
    public void run() {
        init();

        ByteBuffer byteBuffer;
        int tmp;
        long seq;
        ValueEvent valueEvent;
        MessageDataBlock messageDataBlock;
        while (true){

            if (overOfCurrentDataFile){

                //还有没有读取的数据文件
                if (dataFileIndex + 1 < dataFileArray.length){
                    try {
                        fileChannel.close();
                        dataFileIndex ++;
                        fileChannel = new RandomAccessFile(dataFileArray[dataFileIndex], "r").getChannel();
                        fileSize = fileChannel.size();
                        readPosition = 0;
                        overOfCurrentDataFile = false;
                        logger.info("begin to read data file " + dataFileArray[dataFileIndex]);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }else {
                    //所有的数据文件都已经被读取过
                    //跳出循环结束
                    break;
                }
            }

            try {
                fileChannel.position(readPosition);
                if (fileSize - readPosition > SIZE_OF_BYTE_BUFFER){
                    if (flagOfStart){
                        size = size <<1;
                        if (size >= SIZE_OF_BYTE_BUFFER){
                            flagOfStart = false;
                        }
                    }
                    byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, readPosition, size);
                    readPosition += size;
                }else {
                    byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, readPosition, fileSize - readPosition);
                    readPosition = fileSize;
                }
                byteBuffer.position(byteBuffer.limit());

                //修正
                if (readPosition != fileSize){
                    tmp = byteBuffer.position();

                    while (true){
                        readPosition --;
                        tmp -- ;
                        byteBuffer.position(tmp);
                        if (byteBuffer.get()=='\n'){
                            tmp ++;
                            readPosition ++;
                            byteBuffer.position(tmp);
                            break;
                        }
                    }
                }else {
                    //当前所在数据文件的数据已经读取完毕
                    overOfCurrentDataFile = true;
//                    byteBuffer.put((byte) '\n');
                }

                byteBuffer.flip();

                //操作ringBuffer，可能会存在性能问题
                seq = ringBuffer.next();
                valueEvent = ringBuffer.get(seq);

                messageDataBlock = new MessageDataBlock(ringBuffer, seq, valueEvent, byteBuffer);

                //向有入锁和出锁的队列中添加数据
//                logger.info( " size " + messageDataBlockQueue.size());

                messageDataBlockQueue.put(messageDataBlock);

                fileChannel.position(readPosition);

//                logger.info(" position "+readPosition);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        //向每个解析服务分发一个空的byteBuffer，以此来通知解析服务， 数据读取已经完成了
        for (int i=0; i<NUM_OF_PARSE_SERVICE; i++){
            try {
                byteBuffer = ByteBuffer.allocateDirect(8);
                byteBuffer.position(0);
                byteBuffer.flip();

                seq = ringBuffer.next();
                valueEvent = ringBuffer.get(seq);
                messageDataBlock = new MessageDataBlock(ringBuffer, seq, valueEvent, byteBuffer);

                messageDataBlockQueue.put(messageDataBlock);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        //关闭fileChannel
        try {
            fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("data read service is over" + "have read " +  ( 1 + dataFileIndex )  +" data files; ");
    }


}
