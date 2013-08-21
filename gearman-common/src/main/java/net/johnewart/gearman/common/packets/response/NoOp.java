package net.johnewart.gearman.common.packets.response;

import net.johnewart.gearman.constants.PacketType;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 8:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class NoOp extends ResponsePacket {

    public NoOp()
    {
        this.type = PacketType.NOOP;
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
