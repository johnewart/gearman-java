package net.johnewart.gearman.server.net;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import net.johnewart.gearman.server.persistence.PersistenceEngine;
import net.johnewart.gearman.server.storage.JobManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ServerListener {
    private final int port;
    private final Logger LOG = LoggerFactory.getLogger(ServerListener.class);
    private final JobManager jobManager;
    private final NetworkManager networkManager;
    private final boolean enableSSL;

    public ServerListener(int port, PersistenceEngine storageEngine, boolean enableSSL)
    {
        this.port = port;
        this.enableSSL = enableSSL;
        this.jobManager = new JobManager(storageEngine);
        this.networkManager = new NetworkManager(jobManager);

        jobManager.loadAllJobs();

        String host;

        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            host = "localhost";
        }

        LOG.info("Listening on " + host + ":" + port);
    }

    public boolean start()
    {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new GearmanServerInitializer(networkManager, enableSSL))
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.TCP_NODELAY, true);

            bootstrap.bind(port).sync().channel().closeFuture().sync();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

        return true;
    }

    public void stop() {
        LOG.info("Stopping listener...");
    }

    public JobManager getJobManager() {
        return jobManager;
    }
}


/**

 // Bind and start to accept incoming connections.
 bootstrap.setOption("reuseAddress", true);
 bootstrap.setOption("child.tcpNoDelay", true);
 bootstrap.setOption("child.keepAlive", true);
 bootstrap.setPipelineFactory(pipelineFactory);

 Channel channel = bootstrap.bind(new InetSocketAddress(this.port));
 if (!channel.isBound()) {
 this.stop();
 return false;
 }

 this.channelGroup.add(channel);
 return true;


 **/