package net.johnewart.gearman.engine.queue.factories;

import net.johnewart.gearman.engine.exceptions.JobQueueFactoryException;
import net.johnewart.gearman.engine.queue.JobQueue;

public interface JobQueueFactory {
    JobQueue build(String name) throws JobQueueFactoryException;
}
