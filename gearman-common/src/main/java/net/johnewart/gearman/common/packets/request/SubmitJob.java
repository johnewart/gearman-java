package net.johnewart.gearman.common.packets.request;

import net.johnewart.gearman.constants.PacketType;
import net.johnewart.gearman.constants.JobPriority;

import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

public class SubmitJob extends RequestPacket {
    private final AtomicReference<String> taskName, uniqueId, epochString;
    private final byte[] data;
    private final boolean background;
    private final int size;

    public SubmitJob(byte[] pktdata)
    {
        super(pktdata);
        taskName = new AtomicReference<>();
        uniqueId = new AtomicReference<>();
        epochString = new AtomicReference<>();

        int pOff = 0;

        pOff = parseString(pOff, taskName);
        pOff = parseString(pOff, uniqueId);

        if (this.type == PacketType.SUBMIT_JOB_EPOCH)
        {
            pOff = parseString(pOff, epochString);
        }

        this.background = this.type == PacketType.SUBMIT_JOB_HIGH_BG ||
                this.type == PacketType.SUBMIT_JOB_LOW_BG ||
                this.type == PacketType.SUBMIT_JOB_BG ||
                this.type == PacketType.SUBMIT_JOB_EPOCH;

        data = Arrays.copyOfRange(rawdata, pOff, rawdata.length);
        this.size = rawdata.length;
    }

    public SubmitJob(String function, String unique_id, byte[] data, boolean background)
    {
        this(function, unique_id, data, background,JobPriority.NORMAL);
    }

    public SubmitJob(String function, String unique_id, byte[] data, boolean background, JobPriority priority)
    {
        this.taskName = new AtomicReference<>(function);
        this.uniqueId = new AtomicReference<>(unique_id);
        this.epochString = new AtomicReference<>();
        this.background = background;
        this.data = data.clone();

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

        this.size = function.length() + 1 + unique_id.length() + 1 + data.length;
    }

    public Date getWhen()
    {
        return new Date(Long.parseLong(epochString.get()));
    }

    public JobPriority getPriority() {
        switch(this.type)
        {
            case SUBMIT_JOB:
            case SUBMIT_JOB_BG:
            case SUBMIT_JOB_EPOCH:
            case SUBMIT_JOB_SCHED:
                return JobPriority.NORMAL;
            case SUBMIT_JOB_HIGH:
            case SUBMIT_JOB_HIGH_BG:
                return JobPriority.HIGH;
            case SUBMIT_JOB_LOW:
            case SUBMIT_JOB_LOW_BG:
                return JobPriority.LOW;
        }

        return null;
    }

    public String getFunctionName()
    {
        return this.taskName.get();
    }

    public String getUniqueId()
    {
        return uniqueId.get();
    }

    public boolean isBackground() {
        return background;
    }

    public byte[] getData() {
        return data;
    }

    public long getEpoch()
    {
        return Long.parseLong(epochString.get());
    }

    @Override
    public byte[] toByteArray()
    {

        byte[] metadata;
        if(type == PacketType.SUBMIT_JOB_EPOCH)
        {
            metadata = stringsToTerminatedByteArray(taskName.get(),  uniqueId.get(), epochString.get());
        } else {
            metadata = stringsToTerminatedByteArray(taskName.get(),  uniqueId.get());
        }

        return concatByteArrays(getHeader(), metadata, data);
    }

    @Override
    public int getPayloadSize()
    {
      return size;
    }

}

