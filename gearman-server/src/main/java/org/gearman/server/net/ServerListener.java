package org.gearman.server.net;

import org.gearman.server.persistence.PersistenceEngine;
import org.gearman.server.storage.JobManager;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;

public class ServerListener {
    private final int port;
    private final Logger LOG = LoggerFactory.getLogger(ServerListener.class);
    private final JobManager jobManager;
    private final NetworkManager networkManager;
    private final boolean enableSSL;

    private final DefaultChannelGroup channelGroup;
    private final ServerChannelFactory serverFactory;

    public ServerListener(int port, PersistenceEngine storageEngine, boolean enableSSL)
    {
        this.channelGroup = new DefaultChannelGroup(this + "-channelGroup");
        this.serverFactory =  new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                                                                Executors.newCachedThreadPool());
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
        ServerBootstrap bootstrap = new ServerBootstrap(serverFactory);

        // Set up the pipeline factory.
        ChannelPipelineFactory pipelineFactory =
                new GearmanPipelineFactory(networkManager, channelGroup, enableSSL);

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

    }

    public void stop() {
        LOG.info("Stopping listener...");
        if (this.channelGroup != null) {
            this.channelGroup.close();
        }
        if (this.serverFactory != null) {
            this.serverFactory.releaseExternalResources();
        }
    }

    public JobManager getJobManager() {
        return jobManager;
    }
}
