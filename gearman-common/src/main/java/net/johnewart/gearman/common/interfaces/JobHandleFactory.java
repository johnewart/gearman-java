package net.johnewart.gearman.engine.core;

public interface JobHandleFactory {
    public byte[] getNextJobHandle();
}
