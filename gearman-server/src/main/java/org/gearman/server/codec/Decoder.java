package org.gearman.server.codec;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 12/1/12
 * Time: 8:42 PM
 * To change this template use File | Settings | File Templates.
 */
import com.google.common.primitives.Ints;
import org.gearman.common.packets.Packet;
import org.gearman.common.packets.PacketFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Decoder extends ReplayingDecoder<Decoder.DecodingState> {

    private final Logger LOG = LoggerFactory.getLogger(Decoder.class);
    private Packet packet;
    private byte[] packetData;
    private int messageLength;

    public Decoder() {
        this.reset();
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel,
                            ChannelBuffer buffer, DecodingState state)
            throws Exception {
        switch (state) {
            case HEADER:
                byte[] header = new byte[12];
                buffer.readBytes(header, 0, 12);

                messageLength = Ints.fromByteArray(Arrays.copyOfRange(header, 8, 12));
                packetData = Arrays.copyOf(header, messageLength + 12);

                checkpoint(DecodingState.PAYLOAD);
            case PAYLOAD:
                buffer.readBytes(packetData, 12, messageLength);
                try {
                    packet = PacketFactory.packetFromBytes(packetData);
                    LOG.debug("---> " + packet.getType());
                    return packet;
                } finally {
                    this.reset();
                }
            default:
                throw new Exception("Unknown decoding state: " + state);
        }
    }

    private void reset() {
        checkpoint(DecodingState.HEADER);
        this.packet = null;
        this.packetData = null;
        this.messageLength = -1;
    }

    public enum DecodingState {
        HEADER,
        PAYLOAD,
    }
}