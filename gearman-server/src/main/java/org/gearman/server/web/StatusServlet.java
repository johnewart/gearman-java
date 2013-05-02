package org.gearman.server.web;

import java.io.*;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.fasterxml.jackson.core.JsonFactory;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.gearman.server.Job;
import org.gearman.server.JobQueue;
import org.gearman.server.JobStore;
import org.gearman.server.util.JobQueueMonitor;
import org.gearman.server.util.JobQueueSnapshot;
import org.gearman.server.util.SystemSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 */

public class StatusServlet extends HttpServlet {
    private static final String CONTENT_TYPE = "text/html";
    private JobQueueMonitor jobQueueMonitor;
    private JobStore jobStore;
    private static final JsonFactory jsonFactory = new JsonFactory();
    private final Logger LOG = LoggerFactory.getLogger(StatusServlet.class);
    private static Configuration cfg = new Configuration();

    public StatusServlet(JobQueueMonitor jobQueueMonitor, JobStore jobStore)
    {
        this.jobQueueMonitor = jobQueueMonitor;
        this.jobStore = jobStore;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        cfg.setClassForTemplateLoading(StatusServlet.class, "templates");
        cfg.setTemplateUpdateDelay(0);

        final boolean queues = Boolean.parseBoolean(req.getParameter("queues"));
        final String jobQueueName = req.getParameter("jobQueue");

        String templateName;
        try {
            final OutputStream output = resp.getOutputStream();
            OutputStreamWriter wr = new OutputStreamWriter(output);

            if(queues)
            {
                cfg.getTemplate("queues.ftl").process(new StatusView(jobQueueMonitor, jobStore), wr);
            } else {
                if(jobQueueName != null)
                {
                    cfg.getTemplate("queue.ftl").process(new JobQueueStatusView(jobQueueMonitor, jobStore, jobQueueName), wr);
                } else {
                    cfg.getTemplate("index.ftl").process(new StatusView(jobQueueMonitor, jobStore), wr);
                }
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (TemplateException e) {
            e.printStackTrace();
        }

    }


}




