package net.johnewart.gearman.common.packets.response;

import net.johnewart.gearman.common.packets.request.EchoRequest;
import net.johnewart.gearman.constants.PacketType;

import java.util.Arrays;


public class EchoResponse extends ResponsePacket {

    private final byte[] data;

    public EchoResponse(EchoRequest echoRequest)
    {
        this.type = PacketType.ECHO_RES;
        this.data = echoRequest.getData().clone();
    }

    public EchoResponse(byte[] pktdata)
    {
        super(pktdata);
        int pOff = 0;
        this.data = Arrays.copyOfRange(rawdata, pOff, rawdata.length);
        this.type = PacketType.ECHO_RES;
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
