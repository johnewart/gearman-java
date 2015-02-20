package net.johnewart.gearman.engine.queue.factories;

import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.exceptions.JobQueueFactoryException;
import net.johnewart.gearman.engine.queue.JobQueue;
import net.johnewart.gearman.engine.queue.PersistedJobQueue;
import net.johnewart.gearman.engine.queue.persistence.PostgresPersistenceEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;

public class PostgreSQLPersistedJobQueueFactory implements JobQueueFactory {
    private static Logger LOG = LoggerFactory.getLogger(PostgreSQLPersistedJobQueueFactory.class);

    private final PostgresPersistenceEngine postgresQueue;

    public PostgreSQLPersistedJobQueueFactory(String hostname,
                                              int port,
                                              String database,
                                              String user,
                                              String password,
                                              String tableName) throws JobQueueFactoryException {
        try {
            this.postgresQueue = new PostgresPersistenceEngine(hostname, port, database, user, password, tableName);
        } catch (SQLException e) {
            LOG.error("Unable to create PostgreSQL persistence engine: ", e);
            throw new JobQueueFactoryException("Could not create the PostgreSQL persistence engine!");
        }
    }

    @Override
    public JobQueue build(String name) throws JobQueueFactoryException {
        return new PersistedJobQueue(name, postgresQueue);
    }

    @Override
    public Collection<QueuedJob> loadPersistedJobs() {
        return this.postgresQueue.readAll();
    }
}
