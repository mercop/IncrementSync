package com.alibaba.middleware.race.sync;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

/**
 * Created by sang on 2017/6/18.
 */
public class DataSaveInboundHandler extends SimpleChannelInboundHandler {

    private static Logger logger = LoggerFactory.getLogger(Client.class);


    private Client client;

    private FileChannel fileChannel;

    private long position;

    private long length;

    private ByteBuf byteBuf;

    private int count;

    public DataSaveInboundHandler(Client client) {
        this.client = client;
        init();
    }

    // 连接成功后，向server发送消息
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("com.alibaba.middleware.race.sync.ClientDemoInHandler.channelActive");
        String msg = "I am prepared to receive messages";
        ByteBuf encoded = ctx.alloc().buffer(4 * msg.length());
        encoded.writeBytes(msg.getBytes());
        ctx.write(encoded);
        ctx.flush();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        final EventLoop eventLoop = ctx.channel().eventLoop();
        eventLoop.schedule(new Runnable() {
            @Override
            public void run() {
                client.tryConnect(new Bootstrap(), eventLoop);
            }
        }, 1L, TimeUnit.SECONDS);
        super.channelInactive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {

        byteBuf = (ByteBuf) msg;

        count ++;

        if (byteBuf.getByte(byteBuf.readableBytes() -1) == '*'
                && byteBuf.getByte(byteBuf.readableBytes() -2) == '*'
                && byteBuf.getByte(byteBuf.readableBytes() -3) == '*'){
            byteBuf.readBytes(fileChannel, position, byteBuf.readableBytes() -3);
            fileChannel.close();
            logger.info(" have received all the data");
            System.exit(0);
        }
        if (count % 16 == 0){
            logger.info("position " + position + "  capacity " + byteBuf.capacity());
        }
        length = byteBuf.readableBytes();
        byteBuf.readBytes(fileChannel, position, byteBuf.readableBytes());
        position += length;
    }

    private void init(){
        try {
            File file = new File(Constants.RESULT_HOME + "/" + Constants.RESULT_FILE_NAME);
            if (file.exists()){

                fileChannel = new RandomAccessFile(
                        Constants.RESULT_HOME + "/" + Constants.RESULT_FILE_NAME, "rw").getChannel();
                position = 0;

            }else {
                if(!file.getParentFile().exists()) {
                    //如果目标文件所在的目录不存在，则创建父目录
                    logger.info("目标文件所在目录不存在，准备创建它！");
                    System.out.println("目标文件所在目录不存在，准备创建它！");
                    if(!file.getParentFile().mkdirs()) {
                        logger.error("创建目标文件所在目录失败！");
                        System.out.println("创建目标文件所在目录失败！");
                        return;
                    }
                }

                try {
                    if (file.createNewFile()) {
                        logger.info("创建单个文件 成功！");
                        System.out.println("创建单个文件 成功！");
                    } else {
                        logger.error("创建单个文件 失败！");
                        System.out.println("创建单个文件 失败！");
                        return;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }

                fileChannel = new RandomAccessFile(
                        Constants.RESULT_HOME + "/" + Constants.RESULT_FILE_NAME, "rw").getChannel();
                position = 0;

            }
            count = 0;

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}