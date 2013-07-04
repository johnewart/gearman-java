package org.gearman.common.packets.request;

import org.gearman.common.packets.Packet;
import org.gearman.constants.PacketMagic;

public abstract class RequestPacket extends Packet {
    public RequestPacket ()
    {
    }

    public RequestPacket(byte[] fromdata)
    {
        super(fromdata);
    }

    @Override
    public byte[] getMagic()
    {
        return PacketMagic.REQUEST;
    }


}
