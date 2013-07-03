package org.gearman.common.packets.request;

import org.gearman.constants.PacketType;

public class PreSleep extends RequestPacket {

    public PreSleep()
    {
        this.type = PacketType.PRE_SLEEP;
    }

    public PreSleep(byte[] pktdata)
    {
        super(pktdata);
    }


    @Override
    public byte[] toByteArray()
    {
        return getHeader();
    }

    @Override
    public int getPayloadSize()
    {
        return 0;
    }
}
