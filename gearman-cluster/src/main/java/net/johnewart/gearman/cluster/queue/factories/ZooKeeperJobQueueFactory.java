package net.johnewart.gearman.cluster.queue.factories;

import net.johnewart.gearman.cluster.queue.ZooKeeperJobQueue;
import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.exceptions.JobQueueFactoryException;
import net.johnewart.gearman.engine.queue.JobQueue;
import net.johnewart.gearman.engine.queue.factories.JobQueueFactory;
import net.johnewart.gearman.engine.queue.persistence.PersistenceEngine;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

public class ZooKeeperJobQueueFactory implements JobQueueFactory {
    private final Logger LOG = LoggerFactory.getLogger(ZooKeeperJobQueueFactory.class);

    private final PersistenceEngine persistenceEngine;

    public ZooKeeperJobQueueFactory(final PersistenceEngine persistenceEngine) {
        this.persistenceEngine = persistenceEngine;
    }

    public JobQueue build(String name) throws JobQueueFactoryException {
        try {
            return new ZooKeeperJobQueue(name, persistenceEngine);
        } catch (InterruptedException | KeeperException | IOException e) {
            LOG.error("Error building queue: ", e);
            throw new JobQueueFactoryException();
        }

    }

    @Override
    public Collection<QueuedJob> loadPersistedJobs() {
        return new HashSet<QueuedJob>();
    }
}
