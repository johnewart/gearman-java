package org.gearman.server.persistence;

import org.gearman.server.Job;

import java.util.Collection;

public interface PersistenceEngine {
	public void write(Job job) throws Exception;
	public void delete(Job job) throws Exception;
	public void deleteAll() throws Exception;
	public Collection<Job> readAll() throws Exception;
}