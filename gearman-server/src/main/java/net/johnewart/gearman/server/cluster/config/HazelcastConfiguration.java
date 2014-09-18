package net.johnewart.gearman.server.cluster.config;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.LinkedList;
import java.util.List;

public class HazelcastConfiguration {
    private List<String> hosts = new LinkedList<>();
    private HazelcastInstance hazelcast;
    private int port;

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(final List<String> hosts) {
        this.hosts = hosts;
    }

    public HazelcastInstance getHazelcastInstance() {

        if (hazelcast == null) {
            Config cfg = new Config();
            NetworkConfig network = new NetworkConfig();
            network.setPort(getPort());
            network.setPortAutoIncrement(true);
            cfg.setNetworkConfig(network);

            JoinConfig join = network.getJoin();

            if(getHosts() != null && getHosts().size() > 0) {
                System.err.println("Using unicast config!");

                join.getMulticastConfig().setEnabled(false);

                for(String host : getHosts()) {
                    join.getTcpIpConfig().addMember(host);
                }

                join.getTcpIpConfig().setEnabled(true);
            } else {
                System.err.println("Using multicast config!");
                join.getTcpIpConfig().setEnabled(false);
                join.getMulticastConfig().setEnabled(true);
                join.getMulticastConfig().setMulticastTimeoutSeconds(15);
                join.getMulticastConfig().setMulticastGroup("224.2.2.3");
                join.getMulticastConfig().setMulticastPort(54327);
                //cfg.setProperty("hazelcast.initial.min.cluster.size","2");
                //join.getMulticastConfig().setMulticastGroup(MULTICAST_ADDRESS);
                //join.getMulticastConfig().setMulticastPort(PORT_NUMBER);

            }


            //network.getInterfaces().setEnabled(true).addInterface("10.45.67.*");

             /*
            MapConfig mapCfg = new MapConfig();
            mapCfg.setName("testMap");
            mapCfg.setBackupCount(2);
            mapCfg.getMaxSizeConfig().setSize(10000);
            mapCfg.setTimeToLiveSeconds(300);

            MapStoreConfig mapStoreCfg = new MapStoreConfig();
            mapStoreCfg.setClassName("com.hazelcast.examples.DummyStore").setEnabled(true);
            mapCfg.setMapStoreConfig(mapStoreCfg);

            NearCacheConfig nearCacheConfig = new NearCacheConfig();
            nearCacheConfig.setMaxSize(1000).setMaxIdleSeconds(120).setTimeToLiveSeconds(300);
            mapCfg.setNearCacheConfig(nearCacheConfig);

            cfg.addMapConfig(mapCfg);*/

            hazelcast = Hazelcast.newHazelcastInstance(cfg);
        }

        return hazelcast;
    }
}
