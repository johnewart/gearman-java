package net.johnewart.gearman.common.packets.request;

import net.johnewart.gearman.constants.PacketType;

public class GrabJobAll extends RequestPacket {

    public GrabJobAll()
    {
        this.type = PacketType.GRAB_JOB_ALL;
    }

    public GrabJobAll(byte[] pktdata)
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
