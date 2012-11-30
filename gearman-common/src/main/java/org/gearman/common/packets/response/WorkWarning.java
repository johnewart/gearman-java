package org.gearman.common.packets.response;

import org.gearman.constants.PacketType;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 10:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class WorkWarning extends WorkData
{
    public WorkWarning(String jobhandle, byte[] data)
    {
        super(jobhandle, data);
        this.type = PacketType.WORK_WARNING;
    }

    public WorkWarning(byte[] pktdata)
    {
        super(pktdata);
        this.type = PacketType.WORK_WARNING;
    }
}
