package net.johnewart.gearman.cluster.config;

import java.util.List;

public class HazelcastConfiguration {
    private List<String> hosts;

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(final List<String> hosts) {
        this.hosts = hosts;
    }
}
