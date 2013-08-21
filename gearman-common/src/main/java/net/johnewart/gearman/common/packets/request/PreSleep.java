package net.johnewart.gearman.common.packets.request;

import net.johnewart.gearman.constants.PacketType;

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
