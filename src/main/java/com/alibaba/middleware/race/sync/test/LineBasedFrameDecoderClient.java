package com.alibaba.middleware.race.sync.test;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

/**
 * 客户端实现解决粘包的问题
 */
public class LineBasedFrameDecoderClient {
    private static final String CR = System.getProperty("line.separator");

    public void connection(String host,int port) throws InterruptedException {
        //配置NIO线程组
        EventLoopGroup group = new NioEventLoopGroup();
        Bootstrap boot = new Bootstrap();
        boot.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY,true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        socketChannel.pipeline().addLast(new LineBasedFrameDecoder(2048));
                        socketChannel.pipeline().addLast(new StringEncoder(CharsetUtil.UTF_8));
                        socketChannel.pipeline().addLast(new StringDecoder(CharsetUtil.UTF_8));
                        socketChannel.pipeline().addLast(new LineBasedFrameDecoder(1024),new DecoderClientHandler());
                    }
                });

        System.out.println(" 发起异步链接操作 ");
        //发起异步链接操作
        ChannelFuture channelFuture = boot.connect(host, port).sync();

        System.out.println("等待客户端链路关闭");
        //等待客户端链路关闭
        channelFuture.channel().closeFuture().sync();

        System.out.println("group.shutdownGracefully");
        group.shutdownGracefully();

    }

    /**
     * 处理响应结果
     */
    private class DecoderClientHandler extends ChannelInboundHandlerAdapter {

        public int i;

        public DecoderClientHandler() {
            i =0;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            //接受服务器返回的消息
            i ++;
            String content = (String) msg;
            System.out.println("return msg:"+content + "  " + i);
            if (i == 10){
                ctx.close();
            }
        }
    }



    public static void main(String[] args) throws InterruptedException {
        new LineBasedFrameDecoderClient().connection("127.0.0.1",5527);
        System.out.println(" main function is over");
    }
}
