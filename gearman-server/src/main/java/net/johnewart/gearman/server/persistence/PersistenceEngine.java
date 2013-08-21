package net.johnewart.gearman.server.persistence;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.server.core.QueuedJob;

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