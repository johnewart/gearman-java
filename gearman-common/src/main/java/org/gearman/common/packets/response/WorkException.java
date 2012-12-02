package org.gearman.common.packets.response;

import org.gearman.common.packets.request.RequestPacket;
import org.gearman.constants.PacketType;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 10:27 AM
 * To change this template use File | Settings | File Templates.
 */
public class WorkException extends RequestPacket implements WorkResponse
{
    public AtomicReference<String> jobHandle;
    public byte[] exception;

    public WorkException(String jobhandle, byte[] exception)
    {
        this.jobHandle = new AtomicReference<String>(jobhandle);
        this.exception = exception.clone();
        this.type = PacketType.WORK_EXCEPTION;
    }

    public WorkException(byte[] pktdata)
    {
        super(pktdata);
        this.jobHandle = new AtomicReference<String>();
        int pOff = 0;
        pOff = parseString(pOff, jobHandle);
        exception = Arrays.copyOfRange(pktdata, pOff, rawdata.length);
    }

    public byte[] toByteArray()
    {
        return "TODO".getBytes();
    }

    @Override
    public int getPayloadSize()
    {
        return this.jobHandle.get().length() + 1 +
               this.exception.length;
    }

    @Override
    public String getJobHandle()
    {
        return this.jobHandle.get();
    }
}
