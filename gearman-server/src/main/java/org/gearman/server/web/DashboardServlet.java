package org.gearman.server.web;

import java.io.*;

import com.fasterxml.jackson.core.JsonFactory;
import freemarker.template.Configuration;
import freemarker.template.TemplateException;
import org.gearman.server.storage.JobManager;
import org.gearman.server.util.JobQueueMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 */

public class DashboardServlet extends HttpServlet {
    private static final String CONTENT_TYPE = "text/html";
    private JobQueueMonitor jobQueueMonitor;
    private JobManager jobManager;
    private static final JsonFactory jsonFactory = new JsonFactory();
    private final Logger LOG = LoggerFactory.getLogger(DashboardServlet.class);
    private static Configuration cfg = new Configuration();

    public DashboardServlet(JobQueueMonitor jobQueueMonitor, JobManager jobManager)
    {
        this.jobQueueMonitor = jobQueueMonitor;
        this.jobManager = jobManager;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        cfg.setClassForTemplateLoading(DashboardServlet.class, "templates");
        cfg.setTemplateUpdateDelay(0);

        final boolean queues = Boolean.parseBoolean(req.getParameter("queues"));
        final String jobQueueName = req.getParameter("jobQueue");

        String templateName;
        try {
            final OutputStream output = resp.getOutputStream();
            OutputStreamWriter wr = new OutputStreamWriter(output);

            if(queues)
            {
                cfg.getTemplate("queues.ftl").process(new StatusView(jobQueueMonitor, jobManager), wr);
            } else {
                if(jobQueueName != null)
                {
                    cfg.getTemplate("queue.ftl").process(new JobQueueStatusView(jobQueueMonitor, jobManager, jobQueueName), wr);
                } else {
                    cfg.getTemplate("index.ftl").process(new StatusView(jobQueueMonitor, jobManager), wr);
                }
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (TemplateException e) {
            e.printStackTrace();
        }

    }


}




