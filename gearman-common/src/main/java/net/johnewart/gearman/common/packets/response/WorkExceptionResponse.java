package net.johnewart.gearman.common.packets.response;

import net.johnewart.gearman.constants.PacketType;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

public class WorkExceptionResponse extends ResponsePacket implements WorkResponse
{
    public AtomicReference<String> jobHandle;
    public byte[] exception;

    public WorkExceptionResponse(String jobhandle, byte[] exception)
    {
        this.jobHandle = new AtomicReference<String>(jobhandle);
        this.exception = exception.clone();
        this.type = PacketType.WORK_EXCEPTION;
    }

    public WorkExceptionResponse(byte[] pktdata)
    {
        super(pktdata);
        this.jobHandle = new AtomicReference<String>();
        int pOff = 0;
        pOff = parseString(pOff, jobHandle);
        exception = Arrays.copyOfRange(rawdata, pOff, rawdata.length);
    }

    public byte[] toByteArray()
    {
        byte[] metadata = stringsToTerminatedByteArray(jobHandle.get());
        return concatByteArrays(getHeader(), metadata, exception);
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

    public byte[] getException() {
        return exception;
    }
}
