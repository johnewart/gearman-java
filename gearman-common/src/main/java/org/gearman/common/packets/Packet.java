package org.gearman.common.packets;

import com.google.common.primitives.Ints;
import org.gearman.constants.PacketType;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 8:00 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class Packet {
    protected PacketType type;
    protected int size;
    protected byte[] rawdata;
    private byte[] header;

    public Packet() {
        header = null;
    }

    public Packet(byte[] fromdata)
    {
        byte[] typebytes = Arrays.copyOfRange(fromdata, 4, 8);
        byte[] sizebytes = Arrays.copyOfRange(fromdata, 8, 12);

        this.type = PacketType.fromPacketMagicNumber(Ints.fromByteArray(typebytes));
        this.size = Ints.fromByteArray(sizebytes);
        this.rawdata = Arrays.copyOfRange(fromdata, 12, fromdata.length);
    }

    public byte[] getHeader()
    {
        if (header == null)
        {
            byte[] typebytes = Ints.toByteArray(this.type.getIndex());
            byte[] sizebytes = Ints.toByteArray(this.getSize());
            header = concatByteArrays(getMagic(), typebytes, sizebytes);
        }

        return header;
    }

    public PacketType getType()
    {
        return type;
    }

    public abstract byte[] toByteArray();
    public abstract int getSize();
    public abstract byte[] getMagic();

    public String toString()
    {
        //return String.Format("{0} packet. Data: {1} bytes", type.ToString("g"), rawdata.Length);
        return "NARF";
    }

    protected int parseString(int offset, AtomicReference<String> storage)
    {
        int pStart;
        int pOff;
        pOff = pStart = offset;
        for(; pOff < rawdata.length && rawdata[pOff] != 0; pOff++);
        storage.set(new String(Arrays.copyOfRange(rawdata, pStart, pOff)));
        // Return 1 past where we are...
        return pOff + 1;
    }

    protected byte[] stringsToTerminatedByteArray(String first, String... rest)
    {
        StringBuffer fullString = new StringBuffer();
        fullString.append(first);
        fullString.append('\000');
        for(String member : rest)
        {
            fullString.append(member);
            fullString.append('\000');
        }

        return fullString.toString().getBytes();
    }

    protected byte[] concatByteArrays(byte[] first, byte[]... rest) {
        int totalLength = first.length;
        for (byte[] array : rest) {
            totalLength += array.length;
        }
        byte[] result = Arrays.copyOf(first, totalLength);
        int offset = first.length;
        for (byte[] array : rest) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
    }


}
