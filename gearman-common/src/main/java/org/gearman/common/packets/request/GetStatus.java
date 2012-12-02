package org.gearman.common.packets.request;

import org.gearman.constants.PacketType;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 8:48 AM
 * To change this template use File | Settings | File Templates.
 */

public class GetStatus extends RequestPacket
{
    public String jobHandle;

    public GetStatus()
    { }

    public GetStatus(String jobHandle)
    {
        this.jobHandle = jobHandle;
        this.type = PacketType.GET_STATUS;
        this.size = jobHandle.length();
    }

    @Override
    public byte[] toByteArray()
    {
        byte[] jhbytes = jobHandle.getBytes();
        byte[] result = this.concatByteArrays(getHeader(), jhbytes);
        return result;
    }

    @Override
    public int getPayloadSize()
    {
        return this.jobHandle.length();
    }


}
