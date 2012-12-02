package org.gearman.server;

import com.yammer.metrics.HealthChecks;
import com.yammer.metrics.reporting.AdminServlet;
import com.yammer.metrics.reporting.MetricsServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.gearman.server.healthchecks.RedisHealthCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class GearmanDaemon {

	private final Logger LOG = LoggerFactory.getLogger(GearmanDaemon.class);

	public GearmanDaemon(int port) {

        System.err.println("Starting ServerListener...");
        ServerListener sl = new ServerListener(port);
        sl.start();

        System.err.println("Starting Metrics...");
        // Metrics, yo.
        try {
            Server httpServer = new Server(8080);

            HealthChecks.register(new RedisHealthCheck(new Jedis("localhost", 6379)));
            MetricsServlet metricsServlet = new MetricsServlet(true);
            AdminServlet adminServlet = new AdminServlet();
            ServletContextHandler context = new ServletContextHandler();
            context.setContextPath("/");

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
        new GearmanDaemon(4730);
    }
}
