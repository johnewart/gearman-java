package net.johnewart.gearman.common.packets.response;

import net.johnewart.gearman.common.packets.Packet;
import net.johnewart.gearman.constants.PacketMagic;

public abstract class ResponsePacket extends Packet {
    public ResponsePacket()
    {
    }

    public ResponsePacket(byte[] fromdata)
    {
        super(fromdata);
    }

    public byte[] getMagic()
    {
        return PacketMagic.RESPONSE;
    }
}
