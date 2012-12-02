package org.gearman.common.packets.request;

import org.gearman.constants.PacketType;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 8:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class GrabJob extends RequestPacket {

    public GrabJob()
    {
        this.type = PacketType.GRAB_JOB;
    }


    public GrabJob(String function)
    {
        this.type = PacketType.GRAB_JOB;
    }

    public GrabJob(byte[] pktdata)
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
