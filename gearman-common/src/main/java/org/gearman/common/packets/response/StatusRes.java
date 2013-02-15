package org.gearman.common.packets.response;

import org.gearman.constants.PacketType;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 8:46 AM
 * To change this template use File | Settings | File Templates.
 */
public class StatusRes extends ResponsePacket {
    private AtomicReference<String> jobHandle, numeratorStr, denominatorStr;
    private boolean statusKnown;
    private boolean running;
    private int numerator;
    private int denominator;

    public StatusRes()
    {}

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
        jobHandle = new AtomicReference<String>();
        numeratorStr = new AtomicReference<String>();
        denominatorStr = new AtomicReference<String>();

        AtomicReference<String> statusStr = new AtomicReference<String>();
        AtomicReference<String> runningStr = new AtomicReference<String>();

        int pOff = 0;
        pOff = parseString(pOff, jobHandle);
        pOff = parseString(pOff, statusStr);
        pOff = parseString(pOff, runningStr);
        pOff = parseString(pOff, numeratorStr);
        pOff = parseString(pOff, denominatorStr);

        running = Integer.parseInt(runningStr.get()) == 1;
        statusKnown = Integer.parseInt(statusStr.get()) == 1;

        denominator = Integer.parseInt(denominatorStr.get());
        numerator = Integer.parseInt(numeratorStr.get());
    }

    public byte[] toByteArray()
    {
        int knownStatus = statusKnown ? 1 : 0;
        int runningStatus = running ? 1 : 0;
        byte[] metadata = stringsToTerminatedByteArray(
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
        return jobHandle.get().length() + 1 + 4 + 4 + String.valueOf(numerator).length() + 1 + String.valueOf(denominator).length();
    }

    public String toString()
    {
        return jobHandle.get() + ":" + statusKnown + ":" + running + ":" + numerator + ":" + denominator;
    }
}
