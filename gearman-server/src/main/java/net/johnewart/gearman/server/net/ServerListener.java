package net.johnewart.gearman.server.net;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import net.johnewart.gearman.server.config.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerListener {
    private final Logger LOG = LoggerFactory.getLogger(ServerListener.class);

    private final ServerConfiguration serverConfiguration;

    public ServerListener(ServerConfiguration serverConfiguration) {
        this.serverConfiguration = serverConfiguration;
    }

    public boolean start() {
        LOG.info("Listening on " + serverConfiguration.hostName + ":" + serverConfiguration.port);

        LOG.info("Loading existing jobs...");
        // Load up jobs
        serverConfiguration.jobManager.loadAllJobs();

        final NetworkManager networkManager = new NetworkManager(serverConfiguration.jobManager);

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new GearmanServerInitializer(networkManager, serverConfiguration.enableSSL))
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.TCP_NODELAY, true);

            bootstrap.bind(serverConfiguration.port).sync().channel().closeFuture().sync();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }


        return true;
    }

}