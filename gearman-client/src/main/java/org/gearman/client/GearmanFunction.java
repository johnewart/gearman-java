package org.gearman.client;

import org.gearman.common.Job;

public interface GearmanFunction {
    public byte[] process(Job job);
}
