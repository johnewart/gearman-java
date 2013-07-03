package org.gearman.common.packets.request;

import org.gearman.constants.PacketType;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class SetClientId extends RequestPacket {
    private AtomicReference<String> clientId;

    /**
     * Create a SET_CLIENT_ID packet with a random ID
     */
    public SetClientId()
    {
        this(UUID.randomUUID().toString());
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
        parseString(pOff, clientId);
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
        return clientId.get().length();
    }
}
