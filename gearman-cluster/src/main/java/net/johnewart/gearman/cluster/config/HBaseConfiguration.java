package net.johnewart.gearman.cluster.config;

import java.util.List;

public class HBaseConfiguration {
    public List<String> hosts;
    public int port;


    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(List<String> hosts) {
        this.hosts = hosts;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
