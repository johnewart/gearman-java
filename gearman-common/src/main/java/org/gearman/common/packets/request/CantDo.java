package org.gearman.common.packets.request;

import org.gearman.constants.PacketType;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 8:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class CantDo extends RequestPacket {
    private AtomicReference<String> functionName;

    public CantDo()
    {
        this.type = PacketType.CANT_DO;
    }


    public CantDo(String function)
    {
        this.type = PacketType.CAN_DO;
        this.functionName = new AtomicReference<>(function);
    }

    public CantDo(byte[] pktdata)
    {
        super(pktdata);
        this.functionName = new AtomicReference<>();
        int pOff = 0;
        pOff = parseString(pOff, functionName);
    }

    public String getFunctionName()
    {
        return functionName.get();

    }

    @Override
    public byte[] toByteArray()
    {
        return concatByteArrays(getHeader(), functionName.get().getBytes());
    }

    @Override
    public int getPayloadSize()
    {
        return functionName.get().length();
    }
}
