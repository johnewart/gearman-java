package net.johnewart.gearman.engine.core;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.interfaces.EngineClient;

import java.util.Set;

/**
 * Pool of currently running work
 */
public interface JobPool {
    void addJob(Job job);

    void addClientForUniqueId(String uniqueId, EngineClient client);
    void removeClientForUniqueId(String uniqueId, EngineClient client);
    Set<EngineClient> clientsForUniqueId(String uniqueId);

    Job getJobByJobHandle(String jobHandle);
    Job getJobByUniqueId(String uniqueId);

}
