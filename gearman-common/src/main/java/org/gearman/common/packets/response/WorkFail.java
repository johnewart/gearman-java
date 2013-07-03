package org.gearman.common.packets.response;

import org.gearman.common.packets.request.RequestPacket;
import org.gearman.constants.PacketType;

import java.util.concurrent.atomic.AtomicReference;

public class WorkFailResponse extends ResponsePacket implements WorkResponse {
    public AtomicReference<String> jobHandle;

    public WorkFailResponse(String jobhandle)
    {
        this.jobHandle = new AtomicReference<String>(jobhandle);
        this.type = PacketType.WORK_FAIL;
    }

    public WorkFailResponse(byte[] pktdata)
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
    public int getPayloadSize()
    {
        return this.jobHandle.get().length();
    }

    @Override
    public String getJobHandle()
    {
        return this.jobHandle.get();
    }
}
