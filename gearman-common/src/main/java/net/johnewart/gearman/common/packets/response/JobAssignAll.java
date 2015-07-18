package net.johnewart.gearman.common.packets.response;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.constants.PacketType;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

public class JobAssignAll extends ResponsePacket {
    protected AtomicReference<String> jobHandle, functionName;
    protected byte[] data;

    public JobAssignAll(byte[] pktdata)
    {
        super(pktdata);
        jobHandle = new AtomicReference<>();
        functionName = new AtomicReference<>();
        int pOff = parseString(0, jobHandle);
        pOff = parseString(pOff, functionName);
        this.data = Arrays.copyOfRange(rawdata, pOff, rawdata.length);
        this.type = PacketType.JOB_ASSIGN_ALL;
    }

    public JobAssignAll(String jobhandle, String functionName, byte[] data)
    {
        this.jobHandle = new AtomicReference<>(jobhandle);
        this.functionName = new AtomicReference<>(functionName);
        this.data = data.clone();
        this.type = PacketType.JOB_ASSIGN;
    }

    public String getJobHandle()
    {
        return this.jobHandle.get();
    }

    @Override
    public byte[] toByteArray()
    {
        byte[] metadata = stringsToTerminatedByteArray(jobHandle.get(), functionName.get());
        return concatByteArrays(getHeader(), metadata, this.data);
    }

    @Override
    public int getPayloadSize()
    {
        return this.jobHandle.get().length() + 1 +
               this.functionName.get().length() + 1 +
               this.data.length;
    }

    public String getFunctionName() {
        return functionName.get();
    }

    public byte[] getData() {
        return data;
    }

    public Job getJob() {
        return new Job.Builder()
                .jobHandle(this.jobHandle.get())
                .data(this.data)
                .functionName(this.functionName.get())
                .build();
    }
}
