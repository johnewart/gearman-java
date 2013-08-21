package net.johnewart.gearman.client;

import net.johnewart.gearman.common.Job;

public interface GearmanFunction {
    public byte[] process(Job job);
}
