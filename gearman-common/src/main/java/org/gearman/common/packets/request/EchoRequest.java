package org.gearman.common.packets.request;

import org.gearman.constants.GearmanConstants;
import org.gearman.constants.PacketType;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

public class EchoRequest extends RequestPacket {

    private final byte[] data;

    public EchoRequest(String data)
    {
        byte[] dataBytes = data.getBytes(GearmanConstants.CHARSET);
        this.data = dataBytes.clone();
        this.type = PacketType.ECHO_REQ;
    }

    public EchoRequest(byte[] pktdata)
    {
        super(pktdata);
        int pOff = 0;
        this.data = Arrays.copyOfRange(rawdata, pOff, rawdata.length);
        this.type = PacketType.ECHO_REQ;
    }

    @Override
    public byte[] toByteArray()
    {
        return concatByteArrays(getHeader(), data);
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
