package net.johnewart.gearman.engine.queue.factories;

import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.exceptions.JobQueueFactoryException;
import net.johnewart.gearman.engine.queue.JobQueue;
import net.johnewart.gearman.engine.queue.PersistedJobQueue;
import net.johnewart.gearman.engine.queue.persistence.RedisPersistenceEngine;

import java.util.Collection;

public class RedisPersistedJobQueueFactory implements JobQueueFactory {
    private final RedisPersistenceEngine redisQueue;

    public RedisPersistedJobQueueFactory(final String redisHostname, final int redisPort) {
        this.redisQueue = new RedisPersistenceEngine(redisHostname, redisPort);
    }

    @Override
    public JobQueue build(String name) throws JobQueueFactoryException {
        return new PersistedJobQueue(name, redisQueue);
    }

    @Override
    public Collection<QueuedJob> loadPersistedJobs() {
        return redisQueue.readAll();
    }
}
