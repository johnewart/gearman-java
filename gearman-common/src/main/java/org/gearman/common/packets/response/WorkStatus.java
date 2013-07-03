package org.gearman.common.packets.response;

import org.gearman.common.JobStatus;
import org.gearman.common.packets.request.RequestPacket;
import org.gearman.constants.PacketType;

import java.util.concurrent.atomic.AtomicReference;


public class WorkStatus extends ResponsePacket
{
    private AtomicReference<String> jobHandle;
    private int completenumerator;
    private int completedenominator;

    public WorkStatus()
    { }

    public WorkStatus(String jobhandle, int numerator, int denominator)
    {
        this.jobHandle = new AtomicReference<String>(jobhandle);
        this.completenumerator = numerator;
        this.completedenominator = denominator;
        this.type = PacketType.WORK_STATUS;
    }

    public WorkStatus(JobStatus jobStatus)
    {
        this.jobHandle = new AtomicReference<>(jobStatus.getJobHandle());
        this.completedenominator = jobStatus.getDenominator();
        this.completenumerator = jobStatus.getNumerator();
        this.type = PacketType.WORK_STATUS;
    }

    public WorkStatus(byte[] pktdata)
    {
        super(pktdata);
        jobHandle = new AtomicReference<String>();
        AtomicReference<String> numerator = new AtomicReference<String>();
        AtomicReference<String> denominator = new AtomicReference<String>();

        int pOff = 0;
        pOff = parseString(pOff, jobHandle);
        pOff = parseString(pOff, numerator);
        pOff = parseString(pOff, denominator);

        try {
            completedenominator = Integer.valueOf(denominator.get());
        } catch (NumberFormatException nfe) {
            completedenominator = -1;
        }

        try {
            completenumerator = Integer.valueOf(numerator.get());
        } catch (NumberFormatException nfe) {
            completenumerator = -1;
        }
    }

    @Override
    public byte[] toByteArray()
    {
        byte[] metadata = stringsToTerminatedByteArray(false, jobHandle.get(), String.valueOf(completenumerator), String.valueOf(completedenominator));
        return concatByteArrays(getHeader(), metadata);
    }

    @Override
    public int getPayloadSize()
    {
        return this.jobHandle.get().length() + 1 +
               String.valueOf(completenumerator).length() + 1 +
               String.valueOf(completedenominator).length();
    }

    public String getJobHandle() {
        return jobHandle.get();
    }

    public int getCompletenumerator() {
        return completenumerator;
    }

    public int getCompletedenominator() {
        return completedenominator;
    }
}
