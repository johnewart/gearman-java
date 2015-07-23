package net.johnewart.gearman.server.config;

import com.codahale.metrics.MetricRegistry;
import net.johnewart.gearman.common.interfaces.JobHandleFactory;
import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.engine.core.UniqueIdFactory;
import net.johnewart.gearman.engine.metrics.MetricsEngine;
import net.johnewart.gearman.engine.metrics.QueueMetrics;
import net.johnewart.gearman.engine.queue.factories.JobQueueFactory;
import net.johnewart.gearman.engine.queue.factories.MemoryJobQueueFactory;
import net.johnewart.gearman.engine.storage.NoopExceptionStorageEngine;
import net.johnewart.gearman.engine.util.LocalJobHandleFactory;
import net.johnewart.gearman.engine.util.LocalUniqueIdFactory;
import net.johnewart.gearman.server.util.JobQueueMonitor;
import net.johnewart.gearman.server.util.SnapshottingJobQueueMonitor;

import java.net.InetAddress;
import java.net.UnknownHostException;

// Sane defaults.
public class DefaultServerConfiguration extends GearmanServerConfiguration {

    private final JobManager jobManager;
    private final JobQueueFactory jobQueueFactory;
    private final JobQueueMonitor jobQueueMonitor;
    private final JobHandleFactory jobHandleFactory;
    private final UniqueIdFactory uniqueIdFactory;
    private final MetricRegistry registry;

    public DefaultServerConfiguration() {
        this.registry = new MetricRegistry();
        this.jobHandleFactory = new LocalJobHandleFactory(getHostName());
        this.jobQueueFactory = new MemoryJobQueueFactory();
        this.uniqueIdFactory = new LocalUniqueIdFactory();
        this.jobManager = new JobManager(jobQueueFactory, jobHandleFactory, uniqueIdFactory, new NoopExceptionStorageEngine(), getQueueMetrics());
        this.jobQueueMonitor = new SnapshottingJobQueueMonitor(getQueueMetrics());
    }

    @Override
    public int getPort() {
        return 4730;
    }

    @Override
    public int getHttpPort() {
        return 8080;
    }

    @Override
    public boolean isSSLEnabled() {
        return false;
    }

    @Override
    public boolean isDebugging() {
        return false;
    }

    @Override
    public String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }

    @Override
    public JobQueueFactory getJobQueueFactory() {
        return jobQueueFactory;
    }

    @Override
    public JobManager getJobManager() {
        return jobManager;
    }

    @Override
    public JobQueueMonitor getJobQueueMonitor() {
        return jobQueueMonitor;
    }

    @Override
    public JobHandleFactory getJobHandleFactory() {
        return jobHandleFactory;
    }

    @Override
    public UniqueIdFactory getUniqueIdFactory() {
        return uniqueIdFactory;
    }
}
