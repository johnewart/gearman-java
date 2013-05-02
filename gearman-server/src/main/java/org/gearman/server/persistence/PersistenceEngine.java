package org.gearman.server.persistence;

import org.gearman.server.Job;
import org.gearman.server.core.RunnableJob;

import java.util.Collection;

public interface PersistenceEngine {
    public String getIdentifier();
	public void write(Job job);
	public void delete(Job job);
	public void deleteAll();
    public Job findJob(String functionName, String uniqueID);
	public Collection<RunnableJob> readAll();
    public Collection<RunnableJob> getAllForFunction(String functionName);
    public Job findJobByHandle(String jobHandle);
}