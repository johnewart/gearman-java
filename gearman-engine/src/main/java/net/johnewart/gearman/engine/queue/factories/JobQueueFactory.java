package net.johnewart.gearman.engine.queue.factories;

import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.exceptions.JobQueueFactoryException;
import net.johnewart.gearman.engine.queue.JobQueue;

import java.util.Collection;

public interface JobQueueFactory {
    JobQueue build(String name) throws JobQueueFactoryException;
    Collection<QueuedJob> loadPersistedJobs();
}
