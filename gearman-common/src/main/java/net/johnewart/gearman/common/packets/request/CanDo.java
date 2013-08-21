package net.johnewart.gearman.common.packets.request;

import net.johnewart.gearman.constants.PacketType;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 8:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class CanDo extends RequestPacket {
    private AtomicReference<String> functionName;

    public CanDo()
    {
        this.type = PacketType.CAN_DO;
    }


    public CanDo(String function)
    {
        this.type = PacketType.CAN_DO;
        this.functionName = new AtomicReference<>(function);
    }

    public CanDo(byte[] pktdata)
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
