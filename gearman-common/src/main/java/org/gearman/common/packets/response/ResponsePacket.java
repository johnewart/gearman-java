package org.gearman.common.packets.response;

import org.gearman.common.packets.Packet;
import org.gearman.constants.PacketMagic;

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
