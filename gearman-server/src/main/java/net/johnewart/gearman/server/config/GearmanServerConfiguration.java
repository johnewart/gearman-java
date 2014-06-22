package net.johnewart.gearman.server.config;

import net.johnewart.gearman.common.interfaces.JobHandleFactory;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.engine.core.UniqueIdFactory;
import net.johnewart.gearman.engine.queue.factories.JobQueueFactory;
import net.johnewart.gearman.engine.util.LocalJobHandleFactory;
import net.johnewart.gearman.engine.util.LocalUniqueIdFactory;
import net.johnewart.gearman.server.util.SnapshottingJobQueueMonitor;

public class GearmanServerConfiguration implements ServerConfiguration {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(GearmanServerConfiguration.class);

    private int port;
    private int httpPort;
    private boolean enableSSL;
    private boolean debugging;
    private String hostName;
    private JobQueueFactory jobQueueFactory;
    private JobManager jobManager;
    private SnapshottingJobQueueMonitor jobQueueMonitor;
    private PersistenceEngineConfiguration persistenceEngine;

    public void setPort(int port) {
        this.port = port;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public boolean isEnableSSL() {
        return enableSSL;
    }

    public void setEnableSSL(boolean enableSSL) {
        this.enableSSL = enableSSL;
    }

    public void setDebugging(boolean debugging) {
        this.debugging = debugging;

        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        if(debugging)
        {
            root.setLevel(Level.DEBUG);
        } else {
            root.setLevel(Level.ERROR);
        }
    }

    public PersistenceEngineConfiguration getPersistenceEngine() {
        return persistenceEngine;
    }

    public void setPersistenceEngine(PersistenceEngineConfiguration persistenceEngine) {
        this.persistenceEngine = persistenceEngine;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public void setJobQueueFactory(JobQueueFactory jobQueueFactory) {
        this.jobQueueFactory = jobQueueFactory;
    }

    public void setJobManager(JobManager jobManager) {
        this.jobManager = jobManager;
    }

    public void setJobQueueMonitor(SnapshottingJobQueueMonitor jobQueueMonitor) {
        this.jobQueueMonitor = jobQueueMonitor;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public int getHttpPort() {
        return httpPort;
    }

    @Override
    public boolean isSSLEnabled() {
        return enableSSL;
    }

    @Override
    public boolean isDebugging() {
        return debugging;
    }

    @Override
    public String getHostName() {
        if (hostName == null) {
            hostName = "localhost";
        }

        return hostName;
    }

    @Override
    public JobQueueFactory getJobQueueFactory() {
        if (jobQueueFactory == null && getPersistenceEngine() != null) {
            jobQueueFactory = getPersistenceEngine().getJobQueueFactory();
        }

        return jobQueueFactory;
    }

    @Override
    public JobManager getJobManager() {
        if(jobManager == null) {
            jobManager = new JobManager(getJobQueueFactory(), getJobHandleFactory(), getUniqueIdFactory());
        }

        return jobManager;
    }

    @Override
    public SnapshottingJobQueueMonitor getJobQueueMonitor() {
        if (jobQueueMonitor == null) {
            jobQueueMonitor = new SnapshottingJobQueueMonitor(getJobManager());
        }

        return jobQueueMonitor;
    }

    @Override
    public JobHandleFactory getJobHandleFactory() {
        return new LocalJobHandleFactory(getHostName());
    }

    @Override
    public UniqueIdFactory getUniqueIdFactory() {
        return new LocalUniqueIdFactory();
    }
}
