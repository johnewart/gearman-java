package org.gearman.server.persistence;

import org.gearman.server.Job;

import java.util.Collection;

public interface PersistenceEngine {
	public void write(Job job);
	public void delete(Job job);
	public void deleteAll();
    public Job findJob(String functionName, String uniqueID);
	public Collection<Job> readAll();
    public Collection<Job> getAllForFunction(String functionName);
}