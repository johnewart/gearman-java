package net.johnewart.gearman.server;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import net.johnewart.gearman.server.config.DefaultServerConfiguration;
import net.johnewart.gearman.server.config.GearmanServerConfiguration;
import net.johnewart.gearman.server.net.ServerListener;
import net.johnewart.gearman.server.web.WebListener;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import com.beust.jcommander.Parameter;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class GearmanDaemon {
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(GearmanDaemon.class);

	private static class CommandLineOptions {
        @Parameter(names = {"--debug", "-d"}, description = "Turn on debug mode, resulting in a more verbose log output")
        boolean debug = false;

        @Parameter(names = {"--config", "-c"}, description = "Specify file to load configuration")
        String configFile;

        @Parameter(names = {"--help", "-h"}, description = "Prints usage", help = true)
        boolean help;
    }

    public static void main(String... args)
    {
        CommandLineOptions options = new CommandLineOptions();
        JCommander cmd = new JCommander(options);
        cmd.setProgramName("Gearman Server");

        try {
            cmd.parse(args);
            if(options.help) {
                cmd.usage();
                System.exit(0);
            }

        } catch (ParameterException e) {
            cmd.usage();
            System.exit(0);
        }

        final GearmanServerConfiguration serverConfiguration = loadFromConfigOrGenerateDefaultConfig(options.configFile);
        serverConfiguration.setDebugging(options.debug);
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
            LOG.info("Starting Gearman Server with settings from " + configFile + "...");
            LOG.info("Serveer settings: " + serverConfiguration.toString());

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
