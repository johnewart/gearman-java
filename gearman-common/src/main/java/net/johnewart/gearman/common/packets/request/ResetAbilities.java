package net.johnewart.gearman.common.packets.request;

import net.johnewart.gearman.constants.PacketType;

public class ResetAbilities extends RequestPacket {

    public ResetAbilities()
    {
        this.type = PacketType.RESET_ABILITIES;
    }

    public ResetAbilities(byte[] pktdata)
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
