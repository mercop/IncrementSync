package com.alibaba.middleware.race.sync;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by sang on 2017/6/18.
 */
public class ConnectionListener implements ChannelFutureListener {

    private static Logger logger = LoggerFactory.getLogger(Client.class);

    private Client client;
    public ConnectionListener(Client client) {
        this.client = client;
    }
    @Override
    public void operationComplete(ChannelFuture channelFuture) throws Exception {
        if (!channelFuture.isSuccess()) {
            logger.info("Reconnect");
            System.out.println("Reconnect");
            final EventLoop loop = channelFuture.channel().eventLoop();
            loop.schedule(new Runnable() {
                @Override
                public void run() {
                    client.tryConnect(new Bootstrap(), loop);
                }
            }, 1L, TimeUnit.SECONDS);
        }
    }
}
