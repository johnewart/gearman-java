package net.johnewart.gearman.common.interfaces;

import net.johnewart.gearman.common.Job;

public interface GearmanFunction {
    public byte[] process(Job job);
}
