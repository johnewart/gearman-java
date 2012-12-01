package org.gearman.common.packets.response;

import org.gearman.constants.PacketType;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 8:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class JobAssign extends ResponsePacket {
    private AtomicReference<String> jobHandle, functionName;
    private byte[] data;

    public JobAssign()
    {
        jobHandle = new AtomicReference<>();
        this.type = PacketType.JOB_ASSIGN;
    }

    public JobAssign(byte[] pktdata)
    {
        super(pktdata);
        jobHandle = new AtomicReference<>();
        int pOff = parseString(0, jobHandle);
        parseString(pOff, functionName);
        this.data = Arrays.copyOfRange(rawdata, pOff, rawdata.length);
        this.type = PacketType.JOB_ASSIGN;
    }

    public JobAssign(String jobhandle, String functionName, byte[] data)
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
        return concatByteArrays(getHeader(), jobHandle.get().getBytes());
    }

    @Override
    public int getSize()
    {
        return this.jobHandle.get().length();
    }
}
