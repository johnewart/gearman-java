package net.johnewart.gearman.cluster.config;

import java.util.LinkedList;
import java.util.List;

public class ClusterConfiguration {
    public List<String> seedNodes = new LinkedList<>();
    public int clusterPort = 2551;
    public ClusterMode clusterMode = ClusterMode.WRITE_THROUGH;

    public ClusterConfiguration() {
        seedNodes.add("akka.tcp://ClusterSystem@127.0.0.1:2551");
        seedNodes.add("akka.tcp://ClusterSystem@127.0.0.1:2552");
    }
}
