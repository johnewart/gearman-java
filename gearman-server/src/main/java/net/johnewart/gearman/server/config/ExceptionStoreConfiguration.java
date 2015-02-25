package net.johnewart.gearman.server.config;

import net.johnewart.gearman.engine.queue.factories.JobQueueFactory;
import net.johnewart.gearman.engine.queue.factories.MemoryJobQueueFactory;
import net.johnewart.gearman.engine.queue.factories.PostgreSQLPersistedJobQueueFactory;
import net.johnewart.gearman.engine.queue.factories.RedisPersistedJobQueueFactory;
import net.johnewart.gearman.engine.storage.ExceptionStorageEngine;
import net.johnewart.gearman.engine.storage.MemoryExceptionStorageEngine;
import net.johnewart.gearman.engine.storage.NoopExceptionStorageEngine;
import net.johnewart.gearman.engine.storage.PostgresExceptionStorageEngine;
import net.johnewart.gearman.server.config.persistence.PostgreSQLConfiguration;
import net.johnewart.gearman.server.config.persistence.RedisConfiguration;

import java.sql.SQLException;

public class ExceptionStoreConfiguration {

    private static final String ENGINE_MEMORY = "memory";
    private static final String ENGINE_POSTGRES = "postgres";
    private static final int MAX_MEMORY_ENTRIES = 5000;

    private RedisConfiguration redis;
    private PostgreSQLConfiguration postgreSQL;
    private String engine;
    private ExceptionStorageEngine exceptionStorageEngine;

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public PostgreSQLConfiguration getPostgreSQL() {
        return postgreSQL;
    }

    public void setPostgreSQL(PostgreSQLConfiguration postgreSQL) {
        this.postgreSQL = postgreSQL;
    }

    public ExceptionStorageEngine getExceptionStorageEngine() {
        if(exceptionStorageEngine == null) {
            switch (getEngine()) {
                case ENGINE_MEMORY:
                    exceptionStorageEngine = new MemoryExceptionStorageEngine(MAX_MEMORY_ENTRIES);
                    break;
                case ENGINE_POSTGRES:
                    try {
                        exceptionStorageEngine = new PostgresExceptionStorageEngine(
                                postgreSQL.getHost(),
                                postgreSQL.getPort(),
                                postgreSQL.getDbName(),
                                postgreSQL.getUser(),
                                postgreSQL.getPassword(),
                                postgreSQL.getTable()
                        );
                    } catch (SQLException e) {
                        e.printStackTrace();
                        exceptionStorageEngine = new NoopExceptionStorageEngine();
                    }
                    break;
                default:
                    exceptionStorageEngine = new NoopExceptionStorageEngine();
            }
        }

        return exceptionStorageEngine;
    }
}
