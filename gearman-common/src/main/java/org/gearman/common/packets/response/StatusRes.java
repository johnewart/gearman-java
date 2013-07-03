package org.gearman.common.packets.response;

import org.gearman.common.JobStatus;
import org.gearman.constants.PacketType;

import java.util.concurrent.atomic.AtomicReference;

public class StatusRes extends ResponsePacket {
    private final AtomicReference<String> jobHandle;
    private final boolean statusKnown;
    private final boolean running;
    private final int numerator;
    private final int denominator;

    public StatusRes(String jobHandle, boolean running, boolean statusKnown, int numerator, int denominator)
    {
        this.jobHandle = new AtomicReference<>(jobHandle);
        this.running = running;
        this.statusKnown = statusKnown;
        this.numerator = numerator;
        this.denominator = denominator;
        this.type = PacketType.STATUS_RES;
    }

    public StatusRes(byte[] pktdata)
    {
        super(pktdata);
        jobHandle = new AtomicReference<>();
        AtomicReference<String> numeratorStr = new AtomicReference<>();
        AtomicReference<String> denominatorStr = new AtomicReference<>();

        AtomicReference<String> statusStr = new AtomicReference<>();
        AtomicReference<String> runningStr = new AtomicReference<>();

        int pOff = 0;
        pOff = parseString(pOff, jobHandle);
        pOff = parseString(pOff, statusStr);
        pOff = parseString(pOff, runningStr);
        pOff = parseString(pOff, numeratorStr);
        parseString(pOff, denominatorStr);

        if(Integer.parseInt(runningStr.get()) == 1)
        {
            running = true;
        } else {
            running = false;
        }

        if(Integer.parseInt(statusStr.get()) == 1)
        {
            statusKnown = true;
        } else {
            statusKnown = false;
        }

        denominator = Integer.parseInt(denominatorStr.get());
        numerator = Integer.parseInt(numeratorStr.get());
        type = PacketType.STATUS_RES;
    }

    public StatusRes(JobStatus jobStatus) {
        running = jobStatus.isRunning();
        statusKnown = jobStatus.isStatusKnown();
        denominator = jobStatus.getDenominator();
        numerator = jobStatus.getNumerator();
        jobHandle = new AtomicReference<>(jobStatus.getJobHandle());
        type = PacketType.STATUS_RES;
    }

    public byte[] toByteArray()
    {
        int knownStatus = statusKnown ? 1 : 0;
        int runningStatus = running ? 1 : 0;
        byte[] metadata = stringsToTerminatedByteArray(
                false,  // No final terminator
                jobHandle.get(),
                String.valueOf(knownStatus),
                String.valueOf(runningStatus),
                String.valueOf(numerator),
                String.valueOf(denominator)
        );

        return concatByteArrays(getHeader(), metadata);
    }

    public String getJobHandle()
    {
        return jobHandle.get();
    }

    public int getNumerator() {
        return numerator;
    }

    public int getDenominator() {
        return denominator;
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isStatusKnown() {
        return statusKnown;
    }

    public float percentComplete()
    {
        if(denominator != 0)
        {
            return  (float)numerator / (float)denominator;
        } else {
            return 0.0F;
        }
    }

    @Override
    public int getPayloadSize()
    {
        return jobHandle.get().length() + 1 + 2 + 2 + String.valueOf(numerator).length() + 1 + String.valueOf(denominator).length();
    }

    public String toString()
    {
        return jobHandle.get() + ":" + statusKnown + ":" + running + ":" + numerator + ":" + denominator;
    }
}
