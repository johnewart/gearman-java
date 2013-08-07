package org.gearman.common.packets.response;

import org.gearman.common.Job;
import org.gearman.constants.PacketType;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

// TODO: Refactor this class so that there's not so much repeated between this and JobAssign.
public class JobAssignUniq extends ResponsePacket {
    private AtomicReference<String> jobHandle, functionName, uniqueId;
    private byte[] data;

    public JobAssignUniq(byte[] pktdata)
    {
        super(pktdata);
        jobHandle = new AtomicReference<>();
        functionName = new AtomicReference<>();
        uniqueId = new AtomicReference<>();
        int pOff = parseString(0, jobHandle);
        pOff = parseString(pOff, functionName);
        pOff = parseString(pOff, uniqueId);
        this.data = Arrays.copyOfRange(rawdata, pOff, rawdata.length);
        this.type = PacketType.JOB_ASSIGN_UNIQ;
    }

    public JobAssignUniq(String jobhandle, String functionName, String uniqueId, byte[] data)
    {
        this.jobHandle = new AtomicReference<>(jobhandle);
        this.functionName = new AtomicReference<>(functionName);
        this.uniqueId = new AtomicReference<>(uniqueId);
        this.data = data.clone();
        this.type = PacketType.JOB_ASSIGN_UNIQ;
    }

    public String getJobHandle()
    {
        return this.jobHandle.get();
    }

    @Override
    public byte[] toByteArray()
    {
        byte[] metadata = stringsToTerminatedByteArray(jobHandle.get(), functionName.get(), uniqueId.get());
        return concatByteArrays(getHeader(), metadata, data);
    }

    @Override
    public int getPayloadSize()
    {
        return this.jobHandle.get().length() + 1 +
               this.functionName.get().length() + 1 +
               this.uniqueId.get().length() + 1 +
               this.data.length;
    }

    public String getFunctionName() {
        return functionName.get();
    }

    public String getUniqueId() {
        return uniqueId.get();
    }

    public byte[] getData() {
        return data;
    }

    public Job getJob() {
        return new Job.Builder()
                .jobHandle(this.jobHandle.get())
                .data(this.data)
                .functionName(this.functionName.get())
                .uniqueID(this.uniqueId.get())
                .build();
    }
}
