package org.gearman.common.packets.response;

import org.gearman.common.packets.Packet;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 8:33 AM
 * To change this template use File | Settings | File Templates.
 */
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
        byte[] magic = { '\0', 'R', 'E', 'S' };
        return magic;
    }


}
