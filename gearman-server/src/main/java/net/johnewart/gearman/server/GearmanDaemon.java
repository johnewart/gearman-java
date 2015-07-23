package net.johnewart.gearman.server;

import net.johnewart.gearman.server.config.DefaultServerConfiguration;
import net.johnewart.gearman.server.config.GearmanServerConfiguration;
import net.johnewart.gearman.server.net.ServerListener;
import net.johnewart.gearman.server.web.WebListener;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class GearmanDaemon {
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(GearmanDaemon.class);

    public static void main(String... args)
    {
        final String configFile;

        if (args.length != 1) {
            configFile = "config.yml";
        } else {
            configFile = args[0];
        }


        final GearmanServerConfiguration serverConfiguration = loadFromConfigOrGenerateDefaultConfig(configFile);
        final ServerListener serverListener = new ServerListener(serverConfiguration);
        final WebListener webListener = new WebListener(serverConfiguration);

        try {
            webListener.start();
            serverListener.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static GearmanServerConfiguration loadFromConfigOrGenerateDefaultConfig(final String configFile) {
        GearmanServerConfiguration serverConfiguration = null;
        try (InputStream in = Files.newInputStream(Paths.get(configFile))) {
            Yaml yaml = new Yaml();

            serverConfiguration
                    = yaml.loadAs(in, GearmanServerConfiguration.class);

            System.out.println(serverConfiguration.toString());

            LOG.info("Starting Gearman Server with settings from " + configFile + "...");

        } catch (Exception e) {
            LOG.error("Can't load " + configFile + ": ", e);
        }

        if (serverConfiguration == null) {
            LOG.info("Starting Gearman Server with default settings ...");
            serverConfiguration = new DefaultServerConfiguration();
        }

        return serverConfiguration;
    }


}
