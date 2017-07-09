package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 处理client端的请求 Created by wanshao on 2017/5/25.
 */
public class ServerDemoInHandler extends ChannelInboundHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(Server.class);

    private static int timeCount =0;

    private static final String CR = System.getProperty("line.separator");

    private static MessageStore messageStore = MessageStore.getInstance();

    private static long[][] sqlDataArray;

    private int start;

    private int end;

    private int NUM_OF_UPDATE_SERVICE;

    public ServerDemoInHandler(int start, int end, int NUM_OF_UPDATE_SERVICE){
        this.start = start;
        this.end = end;
        this.NUM_OF_UPDATE_SERVICE = NUM_OF_UPDATE_SERVICE;
    }
    /**
     * 根据channel
     *
     * @param ctx
     * @return
     */
    public static String getIPString(ChannelHandlerContext ctx) {
        String ipString = "";
        String socketString = ctx.channel().remoteAddress().toString();
        int colonAt = socketString.indexOf(":");
        ipString = socketString.substring(1, colonAt);
        return ipString;
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {

        Server.getMap().put(getIPString(ctx), ctx.channel());


        while (true) {
            Thread.sleep(10);
            if (messageStore.isOver) {
                break;
            }
        }

        logger.info("********************* begin to send data ********************");

        sqlDataArray = messageStore.getSqlDataArray();
        byte byte_t = '\t';
        byte byte_n = '\n';
        int count = 0;
        Utils utils = new Utils();
        ByteBuf byteBuf = Unpooled.directBuffer(1024 * 1024);
        for (int i = start + 1; i < end; i++) {
            if (sqlDataArray[i] != null) {

                byteBuf.writeBytes(utils.longValueToBytes(i));

                byteBuf.writeByte(byte_t);
                byteBuf.writeBytes(utils.longToBytes(sqlDataArray[i][0]));

                byteBuf.writeByte(byte_t);
                byteBuf.writeBytes(utils.longToBytes(sqlDataArray[i][1]));

                byteBuf.writeByte(byte_t);
                byteBuf.writeBytes(utils.longToBytes(sqlDataArray[i][2]));

                byteBuf.writeByte(byte_t);
                byteBuf.writeBytes(utils.longToBytes(sqlDataArray[i][3]));

                byteBuf.writeByte(byte_t);
                byteBuf.writeBytes(utils.longToBytes(sqlDataArray[i][4]));

                byteBuf.writeByte(byte_n);

            }

        }
        ctx.writeAndFlush(byteBuf);
        ctx.writeAndFlush("***");

        logger.info("*********************** have send all the data ***********************");
    }

}
