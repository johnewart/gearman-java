package net.johnewart.gearman.common.packets.response;

import net.johnewart.gearman.constants.PacketType;

public interface WorkResponse {
    public abstract String getJobHandle();
    public abstract PacketType getType();
}
