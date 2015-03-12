package net.johnewart.gearman.engine.queue.factories;

import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.exceptions.JobQueueFactoryException;
import net.johnewart.gearman.engine.queue.JobQueue;
import net.johnewart.gearman.engine.queue.PersistedJobQueue;
import net.johnewart.gearman.engine.queue.persistence.DynamoDBPersistenceEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;

public class DynamoDBPersistedJobQueueFactory implements JobQueueFactory {
    private static Logger LOG = LoggerFactory.getLogger(DynamoDBPersistedJobQueueFactory.class);

    private final DynamoDBPersistenceEngine dynamoDBPersistenceEngine;

    public DynamoDBPersistedJobQueueFactory(String endpoint,
                                            String user,
                                            String password,
                                            String tableName,
                                            Integer readUnits,
                                            Integer writeUnits) throws JobQueueFactoryException {
        try {
            this.dynamoDBPersistenceEngine =
                    new DynamoDBPersistenceEngine(endpoint, user, password, tableName, readUnits, writeUnits);
        } catch (SQLException e) {
            LOG.error("Unable to create DynamoDB persistence engine: ", e);
            throw new JobQueueFactoryException("Could not create the DynamoDB persistence engine!");
        }
    }

    @Override
    public JobQueue build(String name) throws JobQueueFactoryException {
        return new PersistedJobQueue(name, dynamoDBPersistenceEngine);
    }

    @Override
    public Collection<QueuedJob> loadPersistedJobs() {
        return this.dynamoDBPersistenceEngine.readAll();
    }
}
