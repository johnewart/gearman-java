package org.gearman.common.packets.response;

import org.gearman.constants.PacketType;

import java.util.concurrent.atomic.AtomicReference;

public class NoJob extends ResponsePacket {

    public NoJob()
    {
        this.type = PacketType.NO_JOB;
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
