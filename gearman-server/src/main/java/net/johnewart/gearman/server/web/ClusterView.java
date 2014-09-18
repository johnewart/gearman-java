package net.johnewart.gearman.server.web;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;

import java.util.Set;

public class ClusterView {

    private final HazelcastInstance hazelcast;

    public ClusterView(HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
    }

    public Set<Member> getClusterMembers() {
        return hazelcast.getCluster().getMembers();
    }

    public Set<Partition> getPartitions() {
        return hazelcast.getPartitionService().getPartitions();
    }


}

