package net.johnewart.gearman.cluster.queue.factories;

import com.hazelcast.core.HazelcastInstance;
import net.johnewart.gearman.cluster.queue.HazelcastJobQueue;
import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.exceptions.JobQueueFactoryException;
import net.johnewart.gearman.engine.queue.factories.JobQueueFactory;
import net.johnewart.gearman.engine.queue.persistence.PersistenceEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;

import static java.lang.String.format;

public class HazelcastJobQueueFactory implements JobQueueFactory {
    private final Logger LOG = LoggerFactory.getLogger(HazelcastJobQueueFactory.class);

    private final PersistenceEngine persistenceEngine;
    private final HazelcastInstance hazelcast;

    public HazelcastJobQueueFactory(final PersistenceEngine persistenceEngine, HazelcastInstance hazelcast) {
        LOG.debug(format("Starting HazelcastJobQueueFactory with persistence engine: %s", persistenceEngine));
        this.persistenceEngine = persistenceEngine;
        this.hazelcast = hazelcast;
    }

    public HazelcastJobQueue build(String name) throws JobQueueFactoryException {
        return new HazelcastJobQueue(name, hazelcast);
    }

    @Override
    public Collection<QueuedJob> loadPersistedJobs() {
        return new HashSet<>();
    }
}
