package org.gearman.common.packets.response;

import org.gearman.constants.PacketType;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 8:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class JobCreated extends ResponsePacket {
    public AtomicReference<String> jobHandle;

    public JobCreated ()
    {
        jobHandle = new AtomicReference<String>();
        this.type = PacketType.JOB_CREATED;
    }

    public JobCreated(byte[] pktdata)
    {
        super(pktdata);
        jobHandle = new AtomicReference<String>();
        parseString(0, jobHandle);
        this.type = PacketType.JOB_CREATED;
    }

    public JobCreated(String jobhandle)
    {
        jobHandle = new AtomicReference<String>(jobhandle);
        this.type = PacketType.JOB_CREATED;
    }

    public String getJobHandle()
    {
        return this.jobHandle.get();
    }

    @Override
    public byte[] toByteArray()
    {
        return concatByteArrays(getHeader(), jobHandle.get().getBytes());
    }

    @Override
    public int getPayloadSize()
    {
        return this.jobHandle.get().length();
    }
}
