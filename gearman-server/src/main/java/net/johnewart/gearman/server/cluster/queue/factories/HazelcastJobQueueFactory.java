package net.johnewart.gearman.server.cluster.queue.factories;

import com.hazelcast.core.HazelcastInstance;
import net.johnewart.gearman.server.cluster.queue.HazelcastJobQueue;
import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.exceptions.JobQueueFactoryException;
import net.johnewart.gearman.engine.queue.factories.JobQueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;

import static java.lang.String.format;

public class HazelcastJobQueueFactory implements JobQueueFactory {
    private final Logger LOG = LoggerFactory.getLogger(HazelcastJobQueueFactory.class);

    private final HazelcastInstance hazelcast;

    public HazelcastJobQueueFactory(HazelcastInstance hazelcast) {
        LOG.debug("Starting HazelcastJobQueueFactory");
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
