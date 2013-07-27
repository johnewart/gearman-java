package org.gearman.server.net.codec;

import org.gearman.common.packets.Packet;
import org.gearman.common.packets.request.EchoRequest;
import org.jboss.netty.buffer.ChannelBuffer;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.Assert.assertTrue;

public class EncoderTest {
    @Test
    public void encodesPacketProperly()
    {
        Packet p = new EchoRequest("ok");
        byte[] channelDataBytes = new byte[p.toByteArray().length];
        ChannelBuffer buffer = Encoder.encodePacket(p);
        buffer.getBytes(0, channelDataBytes);
        assertTrue(Arrays.equals(p.toByteArray(), channelDataBytes));
    }

    @Test
    public void encodesStringProperly()
    {
        String stringToEncode = "gearman";
        byte[] channelDataBytes = new byte[stringToEncode.length()];
        ChannelBuffer buffer = Encoder.encodeString(stringToEncode);
        buffer.getBytes(0, channelDataBytes);
        assertTrue(Arrays.equals(stringToEncode.getBytes(), channelDataBytes));
    }
}
