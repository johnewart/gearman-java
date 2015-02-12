package net.johnewart.gearman.server.net;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import net.johnewart.gearman.server.net.ssl.GearmanSslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;

public class GearmanServerInitializer extends ChannelInitializer<SocketChannel> {
    private final NetworkManager networkManager;
    private final boolean enableSSL;
    private final Logger LOG = LoggerFactory.getLogger(GearmanServerInitializer.class);


    public GearmanServerInitializer(NetworkManager networkManager,
                                    boolean enableSSL)
    {
        this.networkManager = networkManager;
        this.enableSSL = enableSSL;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        if(enableSSL)
        {
            LOG.info("Enabling SSL");
            SSLEngine engine =
                    GearmanSslContextFactory.getServerContext().createSSLEngine();
            engine.setUseClientMode(false);
            pipeline.addLast("ssl", new SslHandler(engine));
        }

        pipeline.addLast("decoder", new Decoder());
        pipeline.addLast("encoder", new Encoder());

        pipeline.addLast("handler", new PacketHandler(networkManager));
    }
}
