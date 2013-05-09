package org.gearman.server.persistence;

import org.gearman.common.Job;
import org.gearman.server.core.QueuedJob;

import java.util.Collection;

public interface PersistenceEngine {
    public String getIdentifier();
	public void write(Job job);
	public void delete(Job job);
    public void delete(String functionName, String uniqueID);
	public void deleteAll();
    public Job findJob(String functionName, String uniqueID);
	public Collection<QueuedJob> readAll();
    public Collection<QueuedJob> getAllForFunction(String functionName);
    public Job findJobByHandle(String jobHandle);
}