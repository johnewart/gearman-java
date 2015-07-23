package net.johnewart.gearman.engine.queue.factories;

import com.codahale.metrics.MetricRegistry;
import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.exceptions.JobQueueFactoryException;
import net.johnewart.gearman.engine.queue.JobQueue;
import net.johnewart.gearman.engine.queue.PersistedJobQueue;
import net.johnewart.gearman.engine.queue.persistence.MemoryPersistenceEngine;

import java.util.Collection;
import java.util.LinkedList;

public class MemoryJobQueueFactory implements JobQueueFactory {
    private final MetricRegistry metricRegistry;

    public MemoryJobQueueFactory(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    public JobQueue build(String name) throws JobQueueFactoryException {
        return new PersistedJobQueue(name, new MemoryPersistenceEngine(), metricRegistry);
    }

    @Override
    public Collection<QueuedJob> loadPersistedJobs() {
        return new LinkedList<>();
    }
}
