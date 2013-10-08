package net.johnewart.gearman.cluster.config;

import net.johnewart.gearman.cluster.persistence.HBasePersistenceEngine;
import net.johnewart.gearman.cluster.queue.factories.ZooKeeperJobQueueFactory;
import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.engine.queue.factories.JobQueueFactory;
import net.johnewart.gearman.engine.queue.persistence.PersistenceEngine;
import net.johnewart.gearman.server.config.ServerConfiguration;
import net.johnewart.gearman.server.util.JobQueueMonitor;

public class ClusterConfiguration implements ServerConfiguration {

    private int port;
    private int httpPort;
    private boolean enableSSL;
    private boolean debugging;
    private String hostName;
    private JobQueueFactory jobQueueFactory;
    private JobManager jobManager;
    private JobQueueMonitor jobQueueMonitor;
    private String jobQueuePersistenceEngine;
    private PersistenceEngine persistenceEngine;

    private ZooKeeperConfiguration zooKeeperConfiguration;
    private HBaseConfiguration hBaseConfiguration;

    public ClusterConfiguration() {    }

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
        return hostName;
    }

    @Override
    public JobQueueFactory getJobQueueFactory() {
        if (jobQueueFactory == null) {
            jobQueueFactory = new ZooKeeperJobQueueFactory(persistenceEngine);
        }

        return jobQueueFactory;
    }

    @Override
    public JobManager getJobManager() {
        if (jobManager == null) {
            jobManager = new JobManager(getJobQueueFactory());
        }
        return jobManager;
    }

    @Override
    public JobQueueMonitor getJobQueueMonitor() {
        return jobQueueMonitor;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public void setEnableSSL(boolean enableSSL) {
        this.enableSSL = enableSSL;
    }

    public void setDebugging(boolean debugging) {
        this.debugging = debugging;
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

    public void setJobQueueMonitor(JobQueueMonitor jobQueueMonitor) {
        this.jobQueueMonitor = jobQueueMonitor;
    }

    public void setZooKeeper(final ZooKeeperConfiguration zooKeeperConfiguration) {
        this.zooKeeperConfiguration = zooKeeperConfiguration;
    }

    public void setHBase(final HBaseConfiguration hBaseConfiguration) {
        this.hBaseConfiguration = hBaseConfiguration;

        try {
            persistenceEngine =
                    new HBasePersistenceEngine(hBaseConfiguration.getHosts(), hBaseConfiguration.getPort());
        } catch (Exception e) {
            persistenceEngine = null;
        }

    }

    public String getJobQueuePersistenceEngine() {
        return jobQueuePersistenceEngine;
    }

    public void setJobQueuePersistenceEngine(String jobQueuePersistenceEngine) {
        this.jobQueuePersistenceEngine = jobQueuePersistenceEngine;
    }
}
