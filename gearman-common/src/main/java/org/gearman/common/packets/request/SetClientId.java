package org.gearman.common.packets.request;

import org.gearman.constants.PacketType;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 8:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class SetClientId extends RequestPacket {
    private AtomicReference<String> clientId;

    public SetClientId()
    {
        this.type = PacketType.SET_CLIENT_ID;
    }


    public SetClientId(String clientId)
    {
        this.type = PacketType.SET_CLIENT_ID;
        this.clientId = new AtomicReference<>(clientId);
    }

    public SetClientId(byte[] pktdata)
    {
        super(pktdata);
        this.clientId = new AtomicReference<>();
        int pOff = 0;
        pOff = parseString(pOff, clientId);
    }

    public String getClientId()
    {
        return clientId.get();

    }

    @Override
    public byte[] toByteArray()
    {
        return concatByteArrays(getHeader(), clientId.get().getBytes());

    }

    @Override
    public int getPayloadSize()
    {
        return 0;
    }
}
