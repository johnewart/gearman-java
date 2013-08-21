package net.johnewart.gearman.common.packets.request;

import net.johnewart.gearman.common.packets.Packet;
import net.johnewart.gearman.constants.PacketMagic;

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
