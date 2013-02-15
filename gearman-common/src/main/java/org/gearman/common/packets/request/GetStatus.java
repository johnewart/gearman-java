package org.gearman.common.packets.request;

import org.gearman.constants.PacketType;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 8:48 AM
 * To change this template use File | Settings | File Templates.
 */

public class GetStatus extends RequestPacket
{
    public AtomicReference<String> jobHandle;

    public GetStatus()
    { }

    public GetStatus(String jobHandle)
    {
        this.jobHandle = new AtomicReference<>(jobHandle);
        this.type = PacketType.GET_STATUS;
        this.size = jobHandle.length();
    }

    public GetStatus(byte[] pktdata)
    {
        super(pktdata);
        this.type = PacketType.GET_STATUS;

        jobHandle = new AtomicReference<String>();

        int pOff = 0;
        pOff = parseString(pOff, jobHandle);
    }

    @Override
    public byte[] toByteArray()
    {
        byte[] jhbytes = jobHandle.get().getBytes();
        byte[] result = this.concatByteArrays(getHeader(), jhbytes);
        return result;
    }

    @Override
    public int getPayloadSize()
    {
        return this.jobHandle.get().length();
    }


}
