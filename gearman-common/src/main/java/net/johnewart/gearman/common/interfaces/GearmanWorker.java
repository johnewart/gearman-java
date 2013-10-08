package net.johnewart.gearman.common.interfaces;

/**
 * The client-side gearman worker interface.
 */
public interface GearmanWorker {
    void registerCallback(String method, GearmanFunction function);
    void doWork();
}
