package net.johnewart.gearman.server.config;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.codahale.metrics.MetricRegistry;
import net.johnewart.gearman.common.interfaces.JobHandleFactory;
import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.engine.core.UniqueIdFactory;
import net.johnewart.gearman.engine.metrics.MetricsEngine;
import net.johnewart.gearman.engine.metrics.QueueMetrics;
import net.johnewart.gearman.engine.queue.factories.JobQueueFactory;
import net.johnewart.gearman.engine.storage.ExceptionStorageEngine;
import net.johnewart.gearman.engine.util.LocalJobHandleFactory;
import net.johnewart.gearman.engine.util.LocalUniqueIdFactory;
import net.johnewart.gearman.server.cluster.config.ClusterConfiguration;
import net.johnewart.gearman.server.cluster.core.ClusterJobManager;
import net.johnewart.gearman.server.cluster.queue.factories.HazelcastJobQueueFactory;
import net.johnewart.gearman.server.cluster.util.HazelcastJobHandleFactory;
import net.johnewart.gearman.server.cluster.util.HazelcastUniqueIdFactory;
import net.johnewart.gearman.server.util.JobQueueMonitor;
import net.johnewart.gearman.server.util.SnapshottingJobQueueMonitor;
import org.slf4j.LoggerFactory;

public class GearmanServerConfiguration implements ServerConfiguration {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(GearmanServerConfiguration.class);

    private int port;
    private int httpPort;
    private boolean enableSSL;
    private boolean debugging;
    private String hostName;
    private JobQueueFactory jobQueueFactory;
    private JobManager jobManager;
    private JobQueueMonitor jobQueueMonitor;
    private ExceptionStorageEngine exceptionStorageEngine;
    private PersistenceEngineConfiguration persistenceEngine;
    private ClusterConfiguration clusterConfiguration;
    private ExceptionStoreConfiguration exceptionStoreConfiguration;
    private JobHandleFactory jobHandleFactory;
    private UniqueIdFactory uniqueIdFactory;
    private MetricRegistry metricRegistry;
    private QueueMetrics queueMetrics;

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

    public void setExceptionStore(ExceptionStoreConfiguration exceptionStoreConfiguration) {
        this.exceptionStoreConfiguration = exceptionStoreConfiguration;
    }

    public ExceptionStoreConfiguration getExceptionStore() {
        return exceptionStoreConfiguration;
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
            jobManager = new JobManager(getJobQueueFactory(),
                                        getJobHandleFactory(),
                                        getUniqueIdFactory(),
                                        getExceptionStorageEngine(),
                                        getQueueMetrics());
        }

        return jobManager;
    }

    @Override
    public JobQueueMonitor getJobQueueMonitor() {
        if (jobQueueMonitor == null) {
            jobQueueMonitor = new SnapshottingJobQueueMonitor(getQueueMetrics());
        }

        return jobQueueMonitor;
    }

    @Override
    public JobHandleFactory getJobHandleFactory() {
        if (jobHandleFactory == null) {
            jobHandleFactory = new LocalJobHandleFactory(getHostName());
        }

        return jobHandleFactory;
    }

    @Override
    public UniqueIdFactory getUniqueIdFactory() {
        if (uniqueIdFactory == null) {
            uniqueIdFactory = new LocalUniqueIdFactory();
        }

        return uniqueIdFactory;
    }

    @Override
    public MetricRegistry getMetricRegistry()
    {
        if (metricRegistry == null)
        {
            metricRegistry = new MetricRegistry();
        }
        return metricRegistry;
    }


    public QueueMetrics getQueueMetrics() {
        if (queueMetrics == null)
        {
            queueMetrics = new MetricsEngine(getMetricRegistry());
        }
        return queueMetrics;
    }

    public ExceptionStorageEngine getExceptionStorageEngine() {
        if(exceptionStorageEngine == null && getExceptionStore() != null) {
            this.exceptionStorageEngine = getExceptionStore().getExceptionStorageEngine();
        }

        return exceptionStorageEngine;
    }

    public ClusterConfiguration getCluster() {
        return clusterConfiguration;
    }

    // Setting the cluster configuration forces some settings...
    public void setCluster(ClusterConfiguration clusterConfiguration) {
        this.clusterConfiguration = clusterConfiguration;
        this.jobQueueFactory = new HazelcastJobQueueFactory(clusterConfiguration.getHazelcastInstance());
        this.jobHandleFactory = new HazelcastJobHandleFactory(clusterConfiguration.getHazelcastInstance(), getHostName());
        this.uniqueIdFactory = new HazelcastUniqueIdFactory(clusterConfiguration.getHazelcastInstance());
        this.jobManager = new ClusterJobManager(jobQueueFactory, jobHandleFactory, uniqueIdFactory, clusterConfiguration.getHazelcastInstance(), queueMetrics);
        this.jobQueueMonitor = new SnapshottingJobQueueMonitor(queueMetrics);
    }
}
