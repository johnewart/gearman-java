package net.johnewart.gearman.server.config;

import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.engine.queue.factories.JobQueueFactory;
import net.johnewart.gearman.engine.queue.factories.MemoryJobQueueFactory;
import net.johnewart.gearman.server.util.JobQueueMonitor;
import net.johnewart.gearman.server.util.SnapshottingJobQueueMonitor;

import java.net.InetAddress;
import java.net.UnknownHostException;

// Sane defaults.
public class DefaultServerConfiguration implements ServerConfiguration {

    private final JobManager jobManager;
    private final JobQueueFactory jobQueueFactory;
    private final JobQueueMonitor jobQueueMonitor;

    public DefaultServerConfiguration() {
        this.jobQueueFactory = new MemoryJobQueueFactory();
        this.jobManager = new JobManager(jobQueueFactory);
        this.jobQueueMonitor = new SnapshottingJobQueueMonitor(jobManager);
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
}
