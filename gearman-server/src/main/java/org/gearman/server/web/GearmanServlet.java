package org.gearman.server.web;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.gearman.server.Job;
import org.gearman.server.JobQueue;
import org.gearman.server.JobStore;
import org.gearman.server.core.RunnableJob;
import org.gearman.server.util.JobQueueMonitor;
import org.gearman.server.util.JobQueueSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class GearmanServlet extends HttpServlet {
    private static final String CONTENT_TYPE = "application/json";
    private JobQueueMonitor jobQueueMonitor;
    private JobStore jobStore;
    private static final JsonFactory jsonFactory = new JsonFactory(new ObjectMapper());
    private final Logger LOG = LoggerFactory.getLogger(GearmanServlet.class);

    public GearmanServlet(JobQueueMonitor jobQueueMonitor, JobStore jobStore)
    {
        this.jobQueueMonitor = jobQueueMonitor;
        this.jobStore = jobStore;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        final String classPrefix = req.getParameter("class");
        final boolean pretty = Boolean.parseBoolean(req.getParameter("pretty"));
        final boolean history = Boolean.parseBoolean(req.getParameter("history"));
        final String jobQueueName = req.getParameter("jobQueue");

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setHeader("Access-Control-Allow-Origin", "*");

        resp.setContentType(CONTENT_TYPE);
        final OutputStream output = resp.getOutputStream();
        final JsonGenerator json = jsonFactory.createJsonGenerator(output, JsonEncoding.UTF8);
        if (pretty) {
            json.useDefaultPrettyPrinter();
        }
        json.writeStartObject();
        {
            if(jobQueueName == null)
            {
                writeJobQueueStats(json);
            } else {
                if(history)
                {
                    writeJobQueueSnapshots(jobQueueName, json);
                } else {
                    writeJobQueueDetails(jobQueueName, json);
                }
            }
        }
        json.writeEndObject();
        json.close();
    }

    public void writeJobQueueStats(JsonGenerator json) throws IOException {

        ConcurrentHashMap<String, JobQueue> jobQueues = jobStore.getJobQueues();
        for(String jobQueueName : jobQueues.keySet())
        {
            json.writeNumberField(jobQueueName, jobQueues.get(jobQueueName).size());
        }
    }

    public void writeJobQueueSnapshots(String jobQueueName, JsonGenerator json) throws IOException
    {
        if(jobQueueMonitor != null)
        {
            HashMap<String, List<JobQueueSnapshot>> snapshotMap = jobQueueMonitor.getSnapshots();
            if( snapshotMap != null && snapshotMap.containsKey(jobQueueName))
            {
                List<JobQueueSnapshot> snapshotList = ImmutableList.copyOf(jobQueueMonitor.getSnapshots().get(jobQueueName));
                json.writeFieldName("snapshots");
                json.writeStartArray();
                for(JobQueueSnapshot snapshot : snapshotList)
                {
                    json.writeStartObject();
                    {
                        json.writeNumberField("timestamp", snapshot.getTimestamp().getTime());
                        json.writeNumberField("currentJobs", snapshot.getImmediate());
                        if(snapshot.getFutureJobCounts().keySet().size() > 0)
                        {
                            json.writeFieldName("futureJobs");
                            json.writeStartObject();
                            {
                                for(Integer hour : snapshot.getFutureJobCounts().keySet())
                                {
                                    json.writeNumberField(hour.toString(), snapshot.getFutureJobCounts().get(hour));
                                }
                            }
                            json.writeEndObject();
                        }

                    }
                    json.writeEndObject();
                }
                json.writeEndArray();
            }
        } else {
            json.writeFieldName("snapshots");
            json.writeString("Disabled.");
        }
    }

    public void writeJobQueueDetails(String jobQueueName, JsonGenerator json) throws  IOException {

        if(jobStore.getJobQueues().containsKey(jobQueueName))
        {
            JobQueue jobQueue = jobStore.getJobQueues().get(jobQueueName);
            ImmutableSet<RunnableJob> jobs = ImmutableSet.copyOf(jobQueue.getAllJobs());
            json.writeFieldName("jobs");
            json.writeStartArray();
            for(RunnableJob job : jobs)
            {
                json.writeStartObject();
                {
                    json.writeStringField("unique_id", job.getUniqueID());
                    json.writeNumberField("time_to_run", job.getTimeToRun());
                }
                json.writeEndObject();
            }
            json.writeEndArray();
        }
    }

}
