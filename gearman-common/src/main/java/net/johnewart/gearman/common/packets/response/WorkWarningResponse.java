package net.johnewart.gearman.common.packets.response;

import net.johnewart.gearman.constants.PacketType;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 10:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class WorkWarningResponse extends WorkDataResponse
{
    public WorkWarningResponse(String jobhandle, byte[] data)
    {
        super(jobhandle, data);
        this.type = PacketType.WORK_WARNING;
    }

    public WorkWarningResponse(byte[] pktdata)
    {
        super(pktdata);
        this.type = PacketType.WORK_WARNING;
    }
}
