package net.johnewart.gearman.engine.queue.factories;

import com.codahale.metrics.MetricRegistry;
import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.exceptions.JobQueueFactoryException;
import net.johnewart.gearman.engine.queue.JobQueue;
import net.johnewart.gearman.engine.queue.PersistedJobQueue;
import net.johnewart.gearman.engine.queue.persistence.RedisPersistenceEngine;

import java.util.Collection;

public class RedisPersistedJobQueueFactory implements JobQueueFactory {
    private final RedisPersistenceEngine redisQueue;
    private final MetricRegistry metricRegistry;

    public RedisPersistedJobQueueFactory(final String redisHostname, final int redisPort, MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        this.redisQueue = new RedisPersistenceEngine(redisHostname, redisPort, metricRegistry);
    }

    @Override
    public JobQueue build(String name) throws JobQueueFactoryException {
        return new PersistedJobQueue(name, redisQueue, metricRegistry);
    }

    @Override
    public Collection<QueuedJob> loadPersistedJobs() {
        return redisQueue.readAll();
    }
}
