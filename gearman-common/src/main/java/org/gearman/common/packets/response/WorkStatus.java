package org.gearman.common.packets.response;

import org.gearman.common.packets.request.RequestPacket;
import org.gearman.constants.PacketType;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 10:31 AM
 * To change this template use File | Settings | File Templates.
 */
public class WorkStatus extends RequestPacket
{
    public AtomicReference<String> jobHandle;
    public int completenumerator;
    public int completedenominator;

    public WorkStatus()
    { }

    public WorkStatus(String jobhandle, int numerator, int denominator)
    {
        this.jobHandle = new AtomicReference<String>(jobhandle);
        this.completenumerator = numerator;
        this.completedenominator = denominator;
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

        completedenominator = Integer.valueOf(denominator.get());
        completenumerator = Integer.valueOf(numerator.get());
    }

    @Override
    public byte[] toByteArray()
    {
        byte[] metadata = stringsToTerminatedByteArray(jobHandle.get(), String.valueOf(completenumerator), String.valueOf(completedenominator));
        return concatByteArrays(getHeader(), metadata);
    }

    @Override
    public int getSize()
    {
        return this.jobHandle.get().length() + 1 +
               String.valueOf(completenumerator).length() + 1 +
               String.valueOf(completedenominator).length();
    }
}
