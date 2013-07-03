package org.gearman.common.packets.request;

import org.gearman.constants.PacketType;

import java.util.concurrent.atomic.AtomicReference;

public class GrabJob extends RequestPacket {

    public GrabJob()
    {
        this.type = PacketType.GRAB_JOB;
    }

    public GrabJob(byte[] pktdata)
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
