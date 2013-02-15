package org.gearman.server;

import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import com.yammer.metrics.HealthChecks;
import com.yammer.metrics.reporting.AdminServlet;
import com.yammer.metrics.reporting.MetricsServlet;
import org.apache.commons.cli.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.gearman.server.healthchecks.RedisHealthCheck;
import org.gearman.server.persistence.PersistenceEngine;
import org.gearman.server.persistence.PostgresQueue;
import org.gearman.server.persistence.RedisQueue;
import org.gearman.server.util.JobQueueMonitor;
import org.gearman.server.web.GearmanServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class GearmanDaemon {

	private final Logger LOG = LoggerFactory.getLogger(GearmanDaemon.class);



	public GearmanDaemon(int port, PersistenceEngine storageEngine) {

        LOG.info("Starting ServerListener...");
        ServerListener sl = new ServerListener(port, storageEngine);
        sl.start();

        JobQueueMonitor jobQueueMonitor = new JobQueueMonitor(sl.getJobStore());

        LOG.info("Starting Metrics...");
        // Metrics, yo.
        try {
            Server httpServer = new Server(8087);

            HealthChecks.register(new RedisHealthCheck(new Jedis("localhost", 6379)));
            MetricsServlet metricsServlet = new MetricsServlet(true);
            AdminServlet adminServlet = new AdminServlet();
            GearmanServlet gearmanServlet = new GearmanServlet(jobQueueMonitor);
            ServletContextHandler context = new ServletContextHandler(
                    ServletContextHandler.SESSIONS);
            context.setContextPath("/");

            ServletContainer container = new ServletContainer();
            ServletHolder h = new ServletHolder(container);

            context.addServlet(new ServletHolder(gearmanServlet), "/gearman/*");
            context.addServlet(new ServletHolder(metricsServlet), "/status/*");
            context.addServlet(new ServletHolder(adminServlet), "/admin/*");

            httpServer.setHandler(context);
            httpServer.start();
            httpServer.join();
        } catch (Exception e) {
            System.err.println(e.toString());
        }


    }

    public static void main(String... args)
    {
        Options options = new Options();
        options.addOption("port", true, "port to listen on");
        options.addOption("storage", true, "storage engine to use (redis, postgresql)");

        // PostgreSQL options
        options.addOption("", "postgres-user", true, "PostgreSQL user");
        options.addOption("", "postgres-port", true, "PostgreSQL port");
        options.addOption("", "postgres-pass", true, "PostgreSQL password");
        options.addOption("", "postgres-host", true, "PostgreSQL hostname");
        options.addOption("", "postgres-dbname", true, "PostgreSQL database name");

        // Redis options
        options.addOption("", "redis-host", true, "Redis hostname");
        options.addOption("", "redis-port", true, "Redis port");

        CommandLineParser parser = new PosixParser();

        try {
            int port = 4730;

            CommandLine cmd = parser.parse( options, args);

            if(cmd.hasOption("port"))
            {
                port = Integer.parseInt(cmd.getOptionValue("port"));
            }

            String storageName = cmd.getOptionValue("storage");

            PersistenceEngine storageEngine;

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
                    storageEngine = new RedisQueue();
                    break;

                default:
                    storageEngine = null;
            }

            new GearmanDaemon(port, storageEngine);

        } catch (ParseException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }




    }
}
