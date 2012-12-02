package org.gearman.server.codec;

import org.gearman.common.packets.Packet;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 12/1/12
 * Time: 8:40 PM
 * To change this template use File | Settings | File Templates.
 */
@ChannelHandler.Sharable
public class Encoder extends OneToOneEncoder {

    private static Logger LOG = LoggerFactory.getLogger(Encoder.class);

    public static Encoder getInstance() {
        return InstanceHolder.INSTANCE;
    }

    public static ChannelBuffer encodeMessage(Packet packet)
            throws IllegalArgumentException {

        int size = packet.getSize();
        LOG.debug("<--- " + packet.getType());
        ChannelBuffer buffer = ChannelBuffers.buffer(size);
        byte[] data = packet.toByteArray();
        buffer.writeBytes(data);

        return buffer;
    }

    @Override
    protected Object encode(ChannelHandlerContext channelHandlerContext,
                            Channel channel, Object msg) throws Exception {
        if (msg instanceof Packet) {
            return encodeMessage((Packet) msg);
        } else {
            return msg;
        }
    }

    private static final class InstanceHolder {
        private static final Encoder INSTANCE = new Encoder();
    }

}
