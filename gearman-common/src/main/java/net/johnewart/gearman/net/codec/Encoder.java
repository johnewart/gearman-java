package net.johnewart.gearman.net.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import net.johnewart.gearman.common.packets.Packet;
import net.johnewart.gearman.constants.GearmanConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class Encoder extends MessageToByteEncoder<Object> {

    private static Logger LOG = LoggerFactory.getLogger(Encoder.class);

    public static Encoder getInstance() {
        return InstanceHolder.INSTANCE;
    }

    public static byte[] encodePacket(Packet packet)
            throws IllegalArgumentException {

        LOG.debug("<--- " + packet.getType());
        return packet.toByteArray();
    }

    public static byte[] encodeString(String message)
            throws IllegalArgumentException {
        LOG.debug("<--- " + message);
        return message.getBytes(GearmanConstants.CHARSET);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) {
        if (msg instanceof Packet) {
            out.writeBytes(encodePacket((Packet) msg));
        } else if (msg instanceof String) {
            out.writeBytes(encodeString((String) msg));
        } else {
            LOG.error("Unable to encode this thing: " + msg);
        }
    }

    private static final class InstanceHolder {
        private static final Encoder INSTANCE = new Encoder();
    }

}
