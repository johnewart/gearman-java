package net.johnewart.gearman.common.interfaces;

import net.johnewart.gearman.common.events.WorkEvent;

public interface GearmanFunction {
    public byte[] process(WorkEvent workEvent);
}
