package net.johnewart.gearman.server.config;

import net.johnewart.gearman.common.interfaces.JobHandleFactory;
import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.engine.core.UniqueIdFactory;
import net.johnewart.gearman.engine.exceptions.JobQueueFactoryException;
import net.johnewart.gearman.engine.queue.factories.JobQueueFactory;
import net.johnewart.gearman.server.util.JobQueueMonitor;

public interface ServerConfiguration {
    int getPort();

    int getHttpPort();

    boolean isSSLEnabled();

    boolean isDebugging();

    String getHostName();

    JobQueueFactory getJobQueueFactory();

    JobManager getJobManager();

    JobQueueMonitor getJobQueueMonitor();

    JobHandleFactory getJobHandleFactory();

    UniqueIdFactory getUniqueIdFactory();
}
