package org.gearman.server;

import org.gearman.server.persistence.RedisQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 12/1/12
 * Time: 12:26 AM
 * To change this template use File | Settings | File Templates.
 */
public class ServerListener implements Runnable {
    private final int port;
    private final Logger LOG = LoggerFactory.getLogger(ServerListener.class);
    private final JobStore jobStore;
    private final String hostName;

    public ServerListener(int port)
    {
        this.port = port;
        this.jobStore = new JobStore(new RedisQueue());

        if(jobStore != null)
        {
            jobStore.loadAllJobs();
        }

        String host;

        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            host = "localhost";
        }

        this.hostName = host;
    }

    public void run()
    {
        try {
            ServerSocket socket = new ServerSocket(this.port);
            long connectionCount = 0L;

            while (true)
            {
                Socket clientConn = socket.accept();
                LOG.info("Accepted connection #" + connectionCount + " on " + port);
                connectionCount += 1;
                new ServerThread(clientConn, connectionCount, jobStore, hostName).run();
            }
        } catch (IOException ioe) {
            LOG.error("Problem reading from client: ", ioe);
        }
    }


    /*
     public void run()
    {
        ServerBootstrap bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new GearmanHandler(jobStore, hostName));
            }
        });

        // Bind and start to accept incoming connections.
        bootstrap.bind(new InetSocketAddress(port));
    }
     */
}
