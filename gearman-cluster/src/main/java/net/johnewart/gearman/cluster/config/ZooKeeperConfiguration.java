package net.johnewart.gearman.cluster.config;

import java.util.List;

public class ZooKeeperConfiguration {
    private List<String> hosts;

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(final List<String> hosts) {
        this.hosts = hosts;
    }
}
