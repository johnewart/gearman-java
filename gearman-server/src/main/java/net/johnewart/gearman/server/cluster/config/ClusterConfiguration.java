package net.johnewart.gearman.server.cluster.config;

import com.hazelcast.core.HazelcastInstance;
import net.johnewart.gearman.server.cluster.core.ClusterJobManager;
import net.johnewart.gearman.server.cluster.queue.factories.HazelcastJobQueueFactory;
import net.johnewart.gearman.server.cluster.util.HazelcastJobHandleFactory;
import net.johnewart.gearman.server.cluster.util.HazelcastUniqueIdFactory;
import net.johnewart.gearman.common.interfaces.JobHandleFactory;
import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.engine.core.UniqueIdFactory;
import net.johnewart.gearman.engine.queue.factories.JobQueueFactory;
import net.johnewart.gearman.server.config.ServerConfiguration;
import net.johnewart.gearman.server.util.JobQueueMonitor;

public class ClusterConfiguration  {

    private HazelcastConfiguration hazelcastConfiguration;

    public ClusterConfiguration() {
    }

    public ClusterConfiguration(HazelcastConfiguration config) {
        this.hazelcastConfiguration = config;
    }

    public void setHazelcast(HazelcastConfiguration hazelcastConfiguration) {
        this.hazelcastConfiguration = hazelcastConfiguration;
    }

    public HazelcastConfiguration getHazelcast() {
        return hazelcastConfiguration;
    }

    public HazelcastInstance getHazelcastInstance() {
        return getHazelcast().getHazelcastInstance();
    }
}
