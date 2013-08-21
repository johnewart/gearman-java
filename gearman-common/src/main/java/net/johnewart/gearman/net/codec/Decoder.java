package net.johnewart.gearman.net.codec;

import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.ReplayingDecoder;
import net.johnewart.gearman.common.packets.PacketFactory;
import net.johnewart.util.ByteArray;
import net.johnewart.gearman.common.packets.Packet;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

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
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> decoded)
            throws Exception {


        switch (state()) {
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
                } else {
                    textCommand = new ByteArray(1024);
                    textCommand.push(firstByte);
                }

                checkpoint(DecodingState.PAYLOAD);

            case PAYLOAD:
                if(textCommand == null)
                {

                    buffer.readBytes(packetData, 12, messageLength);
                    try {
                        packet = PacketFactory.packetFromBytes(packetData);
                        LOG.debug("---> " + packet.getType());
                        decoded.add(packet);
                    } finally {
                        this.reset();
                    }
                } else {
                    int textOffset = 1;
                    byte b;
                    do {
                        b = buffer.readByte();
                        textCommand.push(b);
                    } while (b != 10);

                    try {
                        String result = textCommand.toString().trim();
                        LOG.debug("Text command: " + result);
                        decoded.add(result);
                    } finally {
                        this.reset();
                    }
                }
                break;

            default:
                throw new Exception("Unknown decoding state: " + state());
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
        TEXT // Text command
    }
}