package org.gearman.common.packets.response;

import org.gearman.constants.PacketType;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

public class WorkDataResponse extends ResponsePacket implements WorkResponse {

    public AtomicReference<String> jobHandle;
    public byte[] data;

    public WorkDataResponse(String jobhandle, byte[] data)
    {
        this.jobHandle = new AtomicReference<>(jobhandle);
        this.data = data.clone();
        this.type = PacketType.WORK_DATA;
    }

    public WorkDataResponse(byte[] pktdata)
    {
        super(pktdata);
        this.jobHandle = new AtomicReference<>();

        int pOff = 0;
        pOff = parseString(pOff, jobHandle);
        data = Arrays.copyOfRange(rawdata, pOff, rawdata.length);

    }

    @Override
    public byte[] toByteArray()
    {
        byte[] metadata = stringsToTerminatedByteArray(jobHandle.get());
        return concatByteArrays(getHeader(), metadata, data);
    }

    public String getJobHandle() {
        return jobHandle.get();
    }

    public int getPayloadSize()
    {
        return this.jobHandle.get().length() + 1 +
               this.data.length;
    }

    public byte[] getData()
    {
        return data;
    }
}
