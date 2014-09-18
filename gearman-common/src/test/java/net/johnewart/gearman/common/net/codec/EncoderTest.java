package net.johnewart.gearman.common.net.codec;

import net.johnewart.gearman.common.packets.Packet;
import net.johnewart.gearman.common.packets.request.EchoRequest;
import net.johnewart.gearman.net.codec.Encoder;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.Assert.assertTrue;

public class EncoderTest {
    @Test
    public void encodesPacketProperly()
    {
        Packet p = new EchoRequest("ok");
        byte[] channelDataBytes = Encoder.encodePacket(p);
        assertTrue(Arrays.equals(p.toByteArray(), channelDataBytes));
    }

    @Test
    public void encodesStringProperly()
    {
        String stringToEncode = "gearman";
        byte[] channelDataBytes = Encoder.encodeString(stringToEncode);
        assertTrue(Arrays.equals(stringToEncode.getBytes(), channelDataBytes));
    }
}
