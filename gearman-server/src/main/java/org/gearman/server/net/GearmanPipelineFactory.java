package org.gearman.server.net;

import org.gearman.server.net.codec.Decoder;
import org.gearman.server.net.codec.Encoder;
import org.gearman.server.net.ssl.GearmanSslContextFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;

public class GearmanPipelineFactory implements ChannelPipelineFactory {
    private final Logger LOG = LoggerFactory.getLogger(GearmanPipelineFactory.class);

    private final NetworkManager networkManager;
    private final ChannelGroup channelGroup;
    private final boolean enableSSL;

    public GearmanPipelineFactory(NetworkManager networkManager,
                                  ChannelGroup channelGroup,
                                  boolean enableSSL)
    {
        this.networkManager = networkManager;
        this.channelGroup = channelGroup;
        this.enableSSL = enableSSL;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {

        ChannelPipeline pipeline = Channels.pipeline();
        if(enableSSL)
        {
            LOG.info("Enabling SSL");
            SSLEngine engine =
                    GearmanSslContextFactory.getServerContext().createSSLEngine();
            engine.setUseClientMode(false);
            pipeline.addLast("ssl", new SslHandler(engine));
        }

        pipeline.addLast("encoder", Encoder.getInstance());
        pipeline.addLast("decoder", new Decoder());
        pipeline.addLast("handler", new PacketHandler(networkManager, channelGroup));

        return pipeline;
    }
}
