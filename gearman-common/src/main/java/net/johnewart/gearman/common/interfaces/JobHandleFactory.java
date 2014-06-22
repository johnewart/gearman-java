package net.johnewart.gearman.common.interfaces;

public interface JobHandleFactory {
    public byte[] getNextJobHandle();
}
