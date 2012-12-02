package org.gearman.common.packets.request;

import org.gearman.constants.PacketType;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 8:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class PreSleep extends RequestPacket {

    public PreSleep()
    {
        this.type = PacketType.PRE_SLEEP;
    }


    public PreSleep(String function)
    {
        this.type = PacketType.PRE_SLEEP;
    }

    public PreSleep(byte[] pktdata)
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
