package net.johnewart.gearman.server.web;

import java.net.URL;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.reporting.AdminServlet;
import com.yammer.metrics.reporting.MetricsServlet;

import net.johnewart.gearman.server.config.ServerConfiguration;

public class WebListener {
    private final Logger LOG = LoggerFactory.getLogger(WebListener.class);
    private final static String TEMPLATE_PATH = "net/johnewart/gearman/server/web/templates";
    private final ServerConfiguration serverConfiguration;

    public WebListener(ServerConfiguration serverConfiguration) {
        this.serverConfiguration = serverConfiguration;
    }

    public void start() throws Exception {
        LOG.info("Listening on " + ":" + serverConfiguration.getHttpPort());

        final Server httpServer = new Server(serverConfiguration.getHttpPort());
        final HandlerList handlerList = new HandlerList();
        final MetricsServlet metricsServlet = new MetricsServlet(true);

        final AdminServlet adminServlet = new AdminServlet();
        final GearmanServlet gearmanServlet =
                new GearmanServlet(serverConfiguration.getJobQueueMonitor(), serverConfiguration.getJobManager());
        final DashboardServlet dashboardServlet =
                new DashboardServlet(serverConfiguration.getJobQueueMonitor(), serverConfiguration.getJobManager());
        final JobQueueServlet jobQueueServlet =
                new JobQueueServlet(serverConfiguration.getJobManager());

        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final URL templateURL = classLoader.getResource(TEMPLATE_PATH);
        if (templateURL != null) {
            final String webResourceDir = templateURL.toExternalForm();
            final ResourceHandler resourceHandler = new ResourceHandler();
            final ContextHandler resourceContext = new ContextHandler("/static");
            final ServletContextHandler servletHandler = new ServletContextHandler(
                    ServletContextHandler.SESSIONS);

            resourceHandler.setResourceBase(webResourceDir);
            resourceContext.setHandler(resourceHandler);

            servletHandler.setContextPath("/");
            servletHandler.addServlet(new ServletHolder(gearmanServlet), "/gearman/*");
            servletHandler.addServlet(new ServletHolder(jobQueueServlet), "/queues/*");
            servletHandler.addServlet(new ServletHolder(metricsServlet), "/metrics/*");
            servletHandler.addServlet(new ServletHolder(adminServlet),   "/admin/*");
            servletHandler.addServlet(new ServletHolder(dashboardServlet),  "/");

            handlerList.addHandler(resourceContext);
            handlerList.addHandler(servletHandler);

            httpServer.setHandler(handlerList);
            httpServer.start();
        } else {
            throw new IllegalArgumentException("Template path inaccessible / does not exist.");
        }
    }

}
