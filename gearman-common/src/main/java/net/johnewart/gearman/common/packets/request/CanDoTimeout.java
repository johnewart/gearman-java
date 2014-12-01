package net.johnewart.gearman.common.packets.request;

import com.google.common.primitives.Ints;
import net.johnewart.gearman.constants.PacketType;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 8:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class CanDoTimeout extends RequestPacket {
    private AtomicReference<String> functionName;
    private final int timeout;

    public CanDoTimeout()
    {
        this.type = PacketType.CAN_DO_TIMEOUT;
        this.timeout = 0;
    }


    public CanDoTimeout(String function, int timeout)
    {
        this.type = PacketType.CAN_DO_TIMEOUT;
        this.functionName = new AtomicReference<>(function);
        this.timeout = timeout;
    }

    public CanDoTimeout(byte[] pktdata)
    {
        super(pktdata);
        this.functionName = new AtomicReference<>();
        int pOff = 0;
        pOff = parseString(pOff, functionName);
        byte[] timeoutbytes = Arrays.copyOfRange(rawdata, pOff, pOff+4);
        this.timeout = Ints.fromByteArray(timeoutbytes);
    }

    public String getFunctionName()
    {
        return functionName.get();
    }

    @Override
    public byte[] toByteArray()
    {
        return concatByteArrays(getHeader(), functionName.get().getBytes(), String.valueOf(timeout).getBytes());

    }

    @Override
    public int getPayloadSize()
    {
        return functionName.get().length() + 1 + String.valueOf(timeout).length();
    }
}
