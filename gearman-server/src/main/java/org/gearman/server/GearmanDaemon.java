package org.gearman.server;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import com.yammer.metrics.core.HealthCheckRegistry;
import com.yammer.metrics.reporting.AdminServlet;
import com.yammer.metrics.reporting.MetricsServlet;
import org.apache.commons.cli.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.gearman.server.net.ServerListener;
import org.gearman.server.persistence.MemoryQueue;
import org.gearman.server.persistence.PersistenceEngine;
import org.gearman.server.persistence.PostgresQueue;
import org.gearman.server.persistence.RedisQueue;
import org.gearman.server.util.JobQueueMonitor;
import org.gearman.server.web.DashboardServlet;
import org.gearman.server.web.GearmanServlet;
import org.slf4j.LoggerFactory;

public class GearmanDaemon {

	private final org.slf4j.Logger LOG = LoggerFactory.getLogger(GearmanDaemon.class);

	public GearmanDaemon(int port,
                         int webport,
                         PersistenceEngine storageEngine,
                         boolean enableSSL)
    {
        try {
            LOG.info("Starting ServerListener...");

            final ServerListener serverListener = new ServerListener(port, storageEngine, enableSSL);

            final JobQueueMonitor jobQueueMonitor =
                    new JobQueueMonitor(serverListener.getJobStore());

            final Server httpServer = new Server(webport);
            final HandlerList handlerList = new HandlerList();
            final MetricsServlet metricsServlet = new MetricsServlet(true);
            final HealthCheckRegistry healthChecks = new HealthCheckRegistry();

            final AdminServlet adminServlet = new AdminServlet();
            final GearmanServlet gearmanServlet =
                    new GearmanServlet(jobQueueMonitor, serverListener.getJobStore());
            final DashboardServlet dashboardServlet =
                    new DashboardServlet(jobQueueMonitor, serverListener.getJobStore());
            final String webDir =
                    GearmanDaemon.class
                            .getClassLoader()
                            .getResource("org/gearman/server/web/templates")
                            .toExternalForm();

            final ResourceHandler resourceHandler = new ResourceHandler();
            final ContextHandler resourceContext = new ContextHandler("/static");
            final ServletContainer container = new ServletContainer();
            final ServletHolder h = new ServletHolder(container);
            final ServletContextHandler servletHandler = new ServletContextHandler(
                    ServletContextHandler.SESSIONS);

            serverListener.start();

            resourceHandler.setResourceBase(webDir);
            resourceContext.setHandler(resourceHandler);
            servletHandler.setContextPath("/");

            servletHandler.addServlet(new ServletHolder(gearmanServlet), "/gearman/*");
            servletHandler.addServlet(new ServletHolder(metricsServlet), "/metrics/*");
            servletHandler.addServlet(new ServletHolder(adminServlet),   "/admin/*");
            servletHandler.addServlet(new ServletHolder(dashboardServlet),  "/");

            handlerList.addHandler(resourceContext);
            handlerList.addHandler(servletHandler);

            httpServer.setHandler(handlerList);
            httpServer.start();
            httpServer.join();
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    public static void main(String... args)
    {
        PersistenceEngine storageEngine;
        boolean jobMonitorEnabled = true;

        Options options = new Options();

        HelpFormatter formatter = new HelpFormatter();

        options.addOption(null, "port", true, "Port to listen on (default: 4730)");
        options.addOption(null, "storage", true, "Storage engine to use (redis, postgresql), default is memory only");
        options.addOption(null, "web-port", true, "Port for the HTTP service (default: 8080)");

        // PostgreSQL options
        options.addOption(null, "postgres-user", true, "PostgreSQL user");
        options.addOption(null, "postgres-port", true, "PostgreSQL port");
        options.addOption(null, "postgres-pass", true, "PostgreSQL password");
        options.addOption(null, "postgres-host", true, "PostgreSQL hostname");
        options.addOption(null, "postgres-dbname", true, "PostgreSQL database name");

        // TODO: Allow for fine-tuning how much data to keep with monitor

        // Redis options
        options.addOption(null, "redis-host", true, "Redis hostname");
        options.addOption(null, "redis-port", true, "Redis port");

        // SSL configuration
        options.addOption(null, "enable-ssl", false, "Enable SSL");

        // Debug level
        options.addOption(null, "debug", false, "Log debug messages");

        // Help
        options.addOption("h", "help", false, "Display this message");

        CommandLineParser parser = new PosixParser();

        try {
            int port = 4730;
            int webport = 8080;
            boolean debugging = false;
            boolean enableSSL = false;

            CommandLine cmd = parser.parse(options, args);

            if(cmd.hasOption("h") || cmd.hasOption("help"))
            {
                formatter.printHelp("java -jar gearman-server.jar [options]", options );
            } else {

                if(cmd.hasOption("debug"))
                {
                    debugging = true;
                }

                if(cmd.hasOption("enable-ssl"))
                {
                    enableSSL = true;
                }

                if(cmd.hasOption("port"))
                {
                    port = Integer.parseInt(cmd.getOptionValue("port"));
                }

                if(cmd.hasOption("web-port"))
                {
                    webport = Integer.parseInt(cmd.getOptionValue("web-port"));
                }

                String storageName = cmd.getOptionValue("storage");

                if(storageName == null)
                {
                    storageName = "memory";
                }

                switch (storageName)
                {
                    case "postgresql":
                        String pghost     = cmd.getOptionValue("postgres-host");
                        String pgdbname   = cmd.getOptionValue("postgres-dbname");
                        String pguser     = cmd.getOptionValue("postgres-user");
                        String pgpass     = cmd.getOptionValue("postgres-pass");
                        int pgport;

                        try {
                            pgport = Integer.parseInt(cmd.getOptionValue("postgres-port"));
                        } catch (NumberFormatException nfe) {
                            pgport = 5432;
                        }

                        // Some sane defaults
                        if(pghost == null)
                            pghost = "localhost";

                        if(pgdbname == null)
                            pgdbname = "gearman";

                        if(pgpass == null)
                            pgpass =  "gearman";

                        if(pguser == null)
                            pguser = "gearman";

                        if(pgport <= 0)
                            pgport = 5432;

                        storageEngine = new PostgresQueue(pghost, pgport, pgdbname, pguser, pgpass);
                        break;

                    case "redis":
                        String redisHostname = cmd.getOptionValue("redis-host");

                        if (redisHostname == null)
                            redisHostname = "localhost";

                        int redisPort;

                        try {
                            redisPort = Integer.parseInt(cmd.getOptionValue("redis-port"));
                        } catch (NumberFormatException nfe) {
                            redisPort = 6379;
                        }

                        storageEngine = new RedisQueue(redisHostname, redisPort);
                        break;

                    default:
                        storageEngine = new MemoryQueue();
                }

                Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
                if(debugging)
                {
                    // Log debug data
                    root.setLevel(Level.DEBUG);
                } else {
                    // Errors only, please
                    root.setLevel(Level.ERROR);
                }

                new GearmanDaemon(port, webport, storageEngine, enableSSL);
            }

        } catch (ParseException e) {
            formatter.printHelp("java -jar gearman-server.jar", options );
        } catch (IllegalArgumentException e) {
            formatter.printHelp("java -jar gearman-server.jar", options );
        }

    }
}
