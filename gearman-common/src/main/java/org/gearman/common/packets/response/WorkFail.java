package org.gearman.common.packets.response;

import org.gearman.common.packets.request.RequestPacket;
import org.gearman.constants.PacketType;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 10:18 AM
 * To change this template use File | Settings | File Templates.
 */
public class WorkFail extends RequestPacket {
    public AtomicReference<String> jobHandle;

    public WorkFail(String jobhandle)
    {
        this.jobHandle = new AtomicReference<String>(jobhandle);
        this.type = PacketType.WORK_FAIL;
    }

    public WorkFail(byte[] pktdata)
    {
        super(pktdata);
        this.jobHandle = new AtomicReference<String>();
        int pOff = 0;
        parseString(0, jobHandle);
    }

    @Override
    public byte[] toByteArray()
    {
        return concatByteArrays(getHeader(), jobHandle.get().getBytes());
    }

    @Override
    public int getSize()
    {
        return this.jobHandle.get().length();
    }
}
