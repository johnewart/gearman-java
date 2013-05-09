package org.gearman.common.packets.response;

import org.gearman.constants.PacketType;

public interface WorkResponse {
    public abstract String getJobHandle();
    public abstract PacketType getType();
}
