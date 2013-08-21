package net.johnewart.gearman.server.web;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import net.johnewart.gearman.server.storage.JobManager;
import net.johnewart.gearman.server.storage.JobQueue;
import net.johnewart.gearman.server.core.QueuedJob;
import net.johnewart.gearman.server.util.JobQueueMonitor;
import net.johnewart.gearman.server.util.JobQueueSnapshot;
import net.johnewart.gearman.server.util.SystemSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class GearmanServlet extends HttpServlet {
    private static final String CONTENT_TYPE = "application/json";
    private JobQueueMonitor jobQueueMonitor;
    private JobManager jobManager;
    private static final JsonFactory jsonFactory = new JsonFactory(new ObjectMapper());
    private final Logger LOG = LoggerFactory.getLogger(GearmanServlet.class);

    public GearmanServlet(JobQueueMonitor jobQueueMonitor, JobManager jobManager)
    {
        this.jobQueueMonitor = jobQueueMonitor;
        this.jobManager = jobManager;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        final String classPrefix = req.getParameter("class");
        final boolean pretty = Boolean.parseBoolean(req.getParameter("pretty"));
        final boolean history = Boolean.parseBoolean(req.getParameter("history"));
        final boolean systemsnapshots = Boolean.parseBoolean(req.getParameter("system"));

        final String jobQueueName = req.getParameter("jobQueue");

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setHeader("Access-Control-Allow-Origin", "*");

        resp.setContentType(CONTENT_TYPE);
        final OutputStream output = resp.getOutputStream();
        final JsonGenerator json = jsonFactory.createJsonGenerator(output, JsonEncoding.UTF8);

        if (pretty) {
            json.useDefaultPrettyPrinter();
        }

        if(systemsnapshots)
        {
            writeSystemSnapshots(json);
        } else {
            json.writeStartObject();
            {
                if(jobQueueName == null)
                {
                    writeAllJobQueueStats(json);
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
        }

        json.close();
    }

    public void writeSystemSnapshots(JsonGenerator json) throws IOException {
        List<SystemSnapshot> snapshots = new ArrayList<>();
        snapshots.addAll(jobQueueMonitor.getSystemSnapshots());
        json.writeStartArray();
        for(SystemSnapshot snapshot : snapshots)
        {
            json.writeStartObject();
            {
                json.writeNumberField("timestamp", snapshot.getTimestamp().getTime());
                json.writeNumberField("totalQueued", snapshot.getTotalJobsQueued());
                json.writeNumberField("totalProcessed", snapshot.getTotalJobsProcessed());
                json.writeNumberField("diffQueued", snapshot.getJobsQueuedSinceLastSnapshot());
                json.writeNumberField("diffProcessed", snapshot.getJobsProcessedSinceLastSnapshot());
                json.writeNumberField("heapUsed", snapshot.getHeapUsed());
            }
            json.writeEndObject();
        }
        json.writeEndArray();
    }

    public void writeAllJobQueueStats(JsonGenerator json) throws IOException {

        ConcurrentHashMap<String, JobQueue> jobQueues = jobManager.getJobQueues();
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

        if(jobManager.getJobQueues().containsKey(jobQueueName))
        {
            JobQueue jobQueue = jobManager.getJobQueues().get(jobQueueName);
            ImmutableSet<QueuedJob> jobs = ImmutableSet.copyOf(jobQueue.getAllJobs());
            json.writeFieldName("jobs");
            json.writeStartArray();
            for(QueuedJob job : jobs)
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
