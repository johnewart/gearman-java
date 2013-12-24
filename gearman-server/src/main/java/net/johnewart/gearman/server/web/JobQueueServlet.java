package net.johnewart.gearman.server.web;

import java.io.IOException;
import java.io.OutputStream;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.util.ByteArray;

public class JobQueueServlet extends HttpServlet {
    private static final String CONTENT_TYPE = "application/json";
    private final JobManager jobManager;
    private final Logger LOG = LoggerFactory.getLogger(JobQueueServlet.class);

    public JobQueueServlet(JobManager jobManager)
    {
        this.jobManager = jobManager;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        final String jobQueueName = req.getPathInfo().replaceFirst("/", "");
        final Job.Builder jobBuilder = new Job.Builder();
        final JobPriority jobPriority = parsePriority(req.getParameter("priority"));
        final ByteArray data = new ByteArray(req.getParameter("data"));
        final String uniqueId = req.getParameter("uniqueId");

        final Job job =
                jobBuilder.data(data.getBytes())
                          .functionName(jobQueueName)
                          .background(true)
                          .uniqueID(uniqueId)
                          .priority(jobPriority)
                          .build();


        final Job created = jobManager.storeJob(job);

        if (created != null) {
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.setContentType(CONTENT_TYPE);

            final OutputStream output = resp.getOutputStream();
            output.write(new ByteArray(created.getJobHandle()).getBytes());
        } else {
            resp.sendError(500, "Could not create job");
        }
    }


    private JobPriority parsePriority(final String priorityString) {

        JobPriority jobPriority = null;

        try {
            if (priorityString == null || priorityString.equals("")) {
                jobPriority = JobPriority.NORMAL;
            } else {
                jobPriority = JobPriority.valueOf(priorityString);
            }
        } catch (IllegalArgumentException iae) {
            LOG.warn("Invalid job queue priority: " + priorityString);
        }

        if (jobPriority == null) {
            jobPriority = JobPriority.NORMAL;
        }

        return jobPriority;

    }

}
