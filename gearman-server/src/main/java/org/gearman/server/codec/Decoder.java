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
import org.gearman.util.ByteArray;
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
    private ByteArray textCommand;

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
                byte firstByte = buffer.readByte();
                textCommand = null;

                // Binary packets are prefixed with 0
                if(firstByte == 0)
                {
                    header[0] = 0;
                    buffer.readBytes(header, 1, 11);
                    messageLength = Ints.fromByteArray(Arrays.copyOfRange(header, 8, 12));
                    packetData = Arrays.copyOf(header, messageLength + 12);
                    checkpoint(DecodingState.PAYLOAD);
                } else {
                    textCommand = new ByteArray(1024);
                    textCommand.push(firstByte);
                    checkpoint(DecodingState.TEXT);
                }

                return null;

            case PAYLOAD:
                buffer.readBytes(packetData, 12, messageLength);
                try {
                    packet = PacketFactory.packetFromBytes(packetData);
                    LOG.debug("---> " + packet.getType());
                    return packet;
                } finally {
                    this.reset();
                }

            case TEXT:
                if(textCommand != null)
                {
                    int textOffset = 1;
                    byte b;
                    do {
                        b = buffer.readByte();
                        textCommand.push(b);
                    } while (b != 10);

                    try {
                        String result = textCommand.toString().trim();
                        LOG.debug("Text command: " + result);
                        return result;
                    } finally {
                        this.reset();
                    }
                } else {
                    LOG.debug("Unable to process text command...");
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
        TEXT // Text ommand
    }
}