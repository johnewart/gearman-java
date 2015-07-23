package net.johnewart.gearman.engine.queue.factories;

import com.codahale.metrics.MetricRegistry;
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
    private final MetricRegistry metricRegistry;

    public DynamoDBPersistedJobQueueFactory(String endpoint,
                                            String user,
                                            String password,
                                            String tableName,
                                            Integer readUnits,
                                            Integer writeUnits,
                                            MetricRegistry metricRegistry) throws JobQueueFactoryException {
        try {
            this.metricRegistry = metricRegistry;
            this.dynamoDBPersistenceEngine =
                    new DynamoDBPersistenceEngine(endpoint, user, password, tableName, readUnits, writeUnits, metricRegistry);
        } catch (SQLException e) {
            LOG.error("Unable to create DynamoDB persistence engine: ", e);
            throw new JobQueueFactoryException("Could not create the DynamoDB persistence engine!");
        }
    }

    @Override
    public JobQueue build(String name) throws JobQueueFactoryException {
        return new PersistedJobQueue(name, dynamoDBPersistenceEngine, metricRegistry);
    }

    @Override
    public Collection<QueuedJob> loadPersistedJobs() {
        return this.dynamoDBPersistenceEngine.readAll();
    }
}
