package org.gearman.common.packets.request;

import org.gearman.common.packets.Packet;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 8:33 AM
 * To change this template use File | Settings | File Templates.
 */
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
        byte[] magic = { '\0', 'R', 'E', 'Q' };
        return magic;
    }


}
