package org.gearman.common.packets.response;

import org.gearman.constants.PacketType;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 8:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class NoJob extends ResponsePacket {

    public NoJob()
    {
        this.type = PacketType.NO_JOB;
    }

    @Override
    public byte[] toByteArray()
    {
        return getHeader();
    }

    @Override
    public int getPayloadSize()
    {
        return 0;
    }
}
