package net.johnewart.gearman.common.packets.response;

import net.johnewart.gearman.common.packets.request.EchoRequest;
import net.johnewart.gearman.constants.PacketType;

import java.util.Arrays;


public class OptionResponse extends ResponsePacket {

    private final byte[] data;

    public OptionResponse(String option)
    {
        this.type = PacketType.OPTION_RES;
        this.data = option.getBytes();
    }

    @Override
    public byte[] toByteArray()
    {
        byte[] result = concatByteArrays(getHeader(), data);
        return result;
    }

    @Override
    public int getPayloadSize()
    {
        return data.length;
    }


    public byte[] getData() {
        return data;
    }
}
