package org.gearman.common.packets.request;

import org.gearman.constants.JobPriority;
import org.gearman.constants.PacketType;

import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 9:37 AM
 * To change this template use File | Settings | File Templates.
 */
public class SubmitJob extends RequestPacket {
    private AtomicReference<String> taskName, uniqueId, epochString;
    private byte[] data;
    private boolean background;
    private int size;

    public SubmitJob()
    {

    }

    public SubmitJob(byte[] pktdata)
    {
        super(pktdata);
        taskName = new AtomicReference<String>();
        uniqueId = new AtomicReference<String>();
        epochString = new AtomicReference<String>();

        int pOff = 0;

        pOff = parseString(pOff, taskName);
        pOff = parseString(pOff, uniqueId);

        if (this.type == PacketType.SUBMIT_JOB_EPOCH)
        {
            pOff = parseString(pOff, epochString);
        }

        if (this.type == PacketType.SUBMIT_JOB_HIGH_BG || this.type == PacketType.SUBMIT_JOB_LOW_BG || this.type == PacketType.SUBMIT_JOB_BG)
        {
            this.background = true;
        }

        data = Arrays.copyOfRange(pktdata, pOff, pktdata.length);
    }

    public SubmitJob(String function, String unique_id, byte[] data, boolean background)
    {
        this.taskName = new AtomicReference<String>(function);
        this.uniqueId = new AtomicReference<String>(unique_id);
        this.data = data;

        if(background)
        {
            this.type = PacketType.SUBMIT_JOB_BG;
        } else {
            this.type = PacketType.SUBMIT_JOB;
        }

        this.size = function.length() + 1 + unique_id.length() + 1 + data.length;

    }

    public SubmitJob(String function, String unique_id, byte[] data, boolean background, JobPriority priority)
    {
        this(function, unique_id, data, background);

        switch (priority)
        {
            case HIGH:
                this.type = background ? PacketType.SUBMIT_JOB_HIGH_BG : PacketType.SUBMIT_JOB_HIGH;
                break;
            case NORMAL:
                this.type = background ? PacketType.SUBMIT_JOB_BG : PacketType.SUBMIT_JOB;
                break;
            case LOW:
                this.type = background ? PacketType.SUBMIT_JOB_LOW_BG : PacketType.SUBMIT_JOB_LOW;
                break;
            default:
                break;
        }
    }

    public Date getWhen()
    {
        return new Date(Long.parseLong(epochString.get()));
    }


    @Override
    public byte[] toByteArray()
    {
        byte[] metadata = stringsToTerminatedByteArray(taskName.get(),  uniqueId.get());
        byte[] result = concatByteArrays(getHeader(), metadata, data);
        return result;
    }

    @Override
    public int getSize()
    {
      return size;
    }

}
