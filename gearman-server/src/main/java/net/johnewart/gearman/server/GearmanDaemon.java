package net.johnewart.gearman.server;

import net.johnewart.gearman.server.config.ServerConfiguration;
import net.johnewart.gearman.server.net.ServerListener;
import net.johnewart.gearman.server.web.WebListener;
import org.apache.commons.cli.ParseException;
import org.slf4j.LoggerFactory;

import java.io.File;

public class GearmanDaemon {
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(GearmanDaemon.class);

    public static void main(String... args)
    {
        final ServerConfiguration serverConfiguration;

        if (args.length == 1) {
            // File name?
            File configFile = new File(args[0]);
            if (configFile.exists() && configFile.canRead()) {
                serverConfiguration = new ServerConfiguration(configFile);
            } else {
                serverConfiguration = parseCommandLine(args);
            }
        } else {
            serverConfiguration = parseCommandLine(args);
        }

        if (serverConfiguration != null) {
            try {
                LOG.info("Starting ServerListener...");

                final ServerListener serverListener = new ServerListener(serverConfiguration);
                final WebListener webListener = new WebListener(serverConfiguration);

                webListener.start();
                serverListener.start();
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error(e.toString());
            }        } else {
            LOG.error("Unable to configure Gearman system. Check command-line options or config file.");
        }
    }

    private static ServerConfiguration parseCommandLine(String... args) {
        try {
            return new ServerConfiguration(args);
        } catch (ParseException e) {
            return null;
        }
    }

}
