package net.johnewart.gearman.server.core;


import net.johnewart.gearman.common.Job;

/**
 * Active job tracker
 */

public interface JobTracker {
    Job findByUniqueId(String uniqueId);
    Job findByJobHandle(String jobHandle);

}
