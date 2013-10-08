package net.johnewart.gearman.cluster;

import net.johnewart.gearman.cluster.config.ClusterConfiguration;
import net.johnewart.gearman.server.net.ServerListener;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class GearmanClusterServer {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(GearmanClusterServer.class);

    public static void main(String... args)  throws IOException {
        final String configFile;

        if (args.length != 1) {
            configFile = "config.yml";
        } else {
            configFile = args[0];
        }

        Yaml yaml = new Yaml();

        try (InputStream in = Files.newInputStream(Paths.get(configFile))) {
            final ClusterConfiguration clusterConfiguration
                    = yaml.loadAs(in, ClusterConfiguration.class);
            System.out.println(clusterConfiguration.toString());

            LOG.info("Starting Gearman Cluster Server...");

            final ServerListener serverListener = new ServerListener(clusterConfiguration);

            serverListener.start();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.toString());
        }
    }

}