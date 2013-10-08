package net.johnewart.gearman.engine.queue.factories;

import net.johnewart.gearman.engine.exceptions.JobQueueFactoryException;
import net.johnewart.gearman.engine.queue.JobQueue;
import net.johnewart.gearman.engine.queue.PersistedJobQueue;
import net.johnewart.gearman.engine.queue.persistence.PostgresPersistenceEngine;

import java.sql.SQLException;

public class PostgreSQLPersistedJobQueueFactory implements JobQueueFactory {
    private final PostgresPersistenceEngine postgresQueue;

    public PostgreSQLPersistedJobQueueFactory(String hostname,
                                              int port,
                                              String database,
                                              String user,
                                              String password) {
        this.postgresQueue = buildQueue(hostname, port, database, user, password);
    }

    private PostgresPersistenceEngine buildQueue(String hostname,
                                     int port,
                                     String database,
                                     String user,
                                     String password) {
        try {
            return new PostgresPersistenceEngine(hostname, port, database, user, password);
        } catch (SQLException se) {
            return null;
        }
    }

    public JobQueue build(String name) throws JobQueueFactoryException {
        if(postgresQueue == null) {
            throw new JobQueueFactoryException();
        }

        return new PersistedJobQueue(name, postgresQueue);
    }
}
