package net.johnewart.gearman.common.packets.request;

import net.johnewart.gearman.constants.PacketType;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

public class WorkDataRequest extends RequestPacket implements WorkRequest {

    public AtomicReference<String> jobHandle;
    public byte[] data;

    public WorkDataRequest(String jobhandle, byte[] data)
    {
        this.jobHandle = new AtomicReference<>(jobhandle);
        this.data = data.clone();
        this.type = PacketType.WORK_DATA;
    }

    public WorkDataRequest(byte[] pktdata)
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
