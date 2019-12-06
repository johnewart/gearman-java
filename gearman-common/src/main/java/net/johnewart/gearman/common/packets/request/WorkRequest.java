package net.johnewart.gearman.common.packets.request;

import net.johnewart.gearman.constants.PacketType;

public interface WorkRequest {
    public abstract String getJobHandle();
    public abstract PacketType getType();
}
