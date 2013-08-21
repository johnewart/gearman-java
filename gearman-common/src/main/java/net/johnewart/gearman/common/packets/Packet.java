package net.johnewart.gearman.common.packets;

import com.google.common.primitives.Ints;
import net.johnewart.gearman.constants.PacketType;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

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
            byte[] sizebytes = Ints.toByteArray(this.getPayloadSize());
            header = concatByteArrays(getMagic(), typebytes, sizebytes);
        }

        return header.clone();
    }

    public PacketType getType()
    {
        return type;
    }

    public abstract byte[] toByteArray();
    public abstract int getPayloadSize();
    public abstract byte[] getMagic();

    public int getSize()
    {
        return 12 + this.getPayloadSize();
    }

    public String toString()
    {
        //return String.Format("{0} packet. Data: {1} bytes", type.ToString("g"), rawdata.Length);
        return this.type.toString();
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

    protected byte[] stringsToTerminatedByteArray(String first, String ... rest)
    {
        return stringsToTerminatedByteArray(true, first, rest);
    }

    protected byte[] stringsToTerminatedByteArray(boolean terminateFinal, String first, String... rest)
    {
        StringBuffer fullString = new StringBuffer();
        fullString.append(first);
        fullString.append('\000');
        for(int i = 0; i < rest.length; i++)
        {
            String member = rest[i];
            fullString.append(member);
            // If we don't want to terminate the last one, then skip
            // For packets where we don't have any data, we need to
            // not have a null byte at the end. (i.e STATUS_RES)
            if(terminateFinal == true || i < rest.length - 1)
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
