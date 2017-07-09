package com.alibaba.middleware.race.sync;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sang on 2017/6/18.
 */
public class Client
{
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    private final static int port = Constants.SERVER_PORT;
    // idle时间
    private static String ip;

    public static EventLoopGroup loop = new NioEventLoopGroup();

    private static Client client;

    public static void main( String[] args )
    {
        Logger logger = LoggerFactory.getLogger(Client.class);
        logger.info("com.alibaba.middleware.race.sync.Client is running....");

        client = new Client();
        ip = args[0];
        initProperties();
        client.connect();

    }

    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
    }

    public void tryConnect(Bootstrap bootstrap, EventLoopGroup eventLoop) {
        if (bootstrap != null) {
            final DataSaveInboundHandler handler = new DataSaveInboundHandler(this);

            bootstrap.group(eventLoop);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socketChannel) throws Exception {

                    socketChannel.pipeline().addLast(handler);

                }
            });
            bootstrap.remoteAddress(ip, port);

            ChannelFuture channelFuture = bootstrap.connect().addListener(new ConnectionListener(this));

            logger.info("等待客户端链路关闭");
//            System.out.println("等待客户端链路关闭");
            try {
                channelFuture.channel().closeFuture().sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    public void connect() {
        tryConnect(new Bootstrap(), loop);
    }
}