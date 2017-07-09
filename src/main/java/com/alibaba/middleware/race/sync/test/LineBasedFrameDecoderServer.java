package com.alibaba.middleware.race.sync.test;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;

import java.net.InetSocketAddress;

/**
 * 利用LineBasedFrameDecoder和StringDecoder测试TCP的粘包问题
 */
public class LineBasedFrameDecoderServer {
    private static final String CR = System.getProperty("line.separator");

    public void server(String host,int port) throws InterruptedException {
        //配置俩组NIO线程组,一组用于数据传输,一组用于接受请求
        //NioEventLoopGroup实际上就是Reactor线程池,负责调度和执行客户端的介入
        //网络读写事件的处理、用户自定义方法和定时任务的执行
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup =  new NioEventLoopGroup();

        try {
            //启动并绑定监听
            ServerBootstrap boot = new ServerBootstrap();
            boot.group(bossGroup, workerGroup)
                    //绑定服务端的类型
                    .channel(NioServerSocketChannel.class)
                    //设定套接字参数
                    //backlog表示为此套接字排队的最大连接数
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    //绑定真正的请求处理类
                    .childHandler(new ChannelInitializer<SocketChannel>(){
                        @Override
                        protected void initChannel(SocketChannel channel) throws Exception {
                            //添加行解码器
                            channel.pipeline().addLast(new LineBasedFrameDecoder(2048));
                            //添加字符编码器
                            channel.pipeline().addLast(new StringEncoder(CharsetUtil.UTF_8));
                            //添加字符解码器
                            channel.pipeline().addLast(new StringDecoder(CharsetUtil.UTF_8));
                            //添加业务处理类
                            channel.pipeline().addLast( new DecoderServerInHandler());
                        }
                    });
            //事实端口绑定,并等待同步成功
            ChannelFuture future = boot.bind(new InetSocketAddress(host, port)).sync();

            System.out.println("TimeServer is started in port:"+port);
            //等待服务器端口监听关闭
            future.channel().closeFuture().sync();

        }finally {
            //是否线程资源
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private class DecoderServerInHandler extends ChannelInboundHandlerAdapter{
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            //读取信息
            String content = (String)msg;
            System.out.println("server receive:"+content);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            //给服务器发送消息
            //           ByteBuf msg  = Unpooled.copiedBuffer("QUERY TIME ORDER\\n".getBytes());
//            ctx.writeAndFlush(msg);
            //下面的操作会出现TCP粘包的情况
            while (true){
                ByteBuf msg  = Unpooled.copiedBuffer(("QUERY TIME ORDER 你好" + CR).getBytes());
                ctx.write(msg);
                ctx.flush();

            }

        }
    }


    public static void main(String[] args) throws InterruptedException {
        new LineBasedFrameDecoderServer().server("127.0.0.1", 5527);
    }
}