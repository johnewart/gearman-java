package net.johnewart.gearman.common.packets.request;

import net.johnewart.gearman.constants.PacketType;

public class WorkCompleteRequest extends WorkDataRequest {
    public WorkCompleteRequest(String jobhandle, byte[] data)
    {
        super(jobhandle, data);
        this.type = PacketType.WORK_COMPLETE;
    }

    public WorkCompleteRequest(byte[] pktdata)
    {
        super(pktdata);
        this.type = PacketType.WORK_COMPLETE;
    }
}