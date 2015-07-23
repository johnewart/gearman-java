package net.johnewart.gearman.server.web;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.metrics.QueueMetrics;
import net.johnewart.gearman.engine.queue.JobQueue;
import net.johnewart.gearman.server.util.JobQueueMetrics;
import net.johnewart.gearman.server.util.JobQueueMonitor;
import net.johnewart.gearman.server.util.SystemSnapshot;
import net.johnewart.shuzai.Frequency;
import net.johnewart.shuzai.SampleMethod;
import net.johnewart.shuzai.TimeSeries;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class GearmanServlet extends HttpServlet {
    private static final String CONTENT_TYPE = "application/json";
    private static final JsonFactory jsonFactory = new JsonFactory(new ObjectMapper());
    private final Logger LOG = LoggerFactory.getLogger(GearmanServlet.class);

    private final JobQueueMonitor jobQueueMonitor;
    private final JobManager jobManager;
    private final QueueMetrics jobQueueMetrics;

    public GearmanServlet(JobQueueMonitor jobQueueMonitor, JobManager jobManager, QueueMetrics jobQueueMetrics)
    {
        this.jobQueueMonitor = jobQueueMonitor;
        this.jobManager = jobManager;
        this.jobQueueMetrics = jobQueueMetrics;
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

                        final DateTime startTime, endTime;
                        if (req.getParameter("start") != null) {
                            startTime = new DateTime(Long.valueOf(req.getParameter("start")));
                        } else {
                            startTime = DateTime.now().minusHours(8);
                        }

                        if (req.getParameter("end") != null) {
                            endTime = new DateTime(Long.valueOf(req.getParameter("end")));
                        } else {
                            endTime = DateTime.now();
                        }

                        writeJobQueueSnapshots(jobQueueName, json, startTime, endTime);
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
        if (jobQueueMonitor != null) {
            snapshots.addAll(jobQueueMonitor.getSystemSnapshots());
        }
        json.writeStartObject();
        {
            json.writeFieldName("snapshots");
            json.writeStartArray();
            for (SystemSnapshot snapshot : snapshots) {
                json.writeStartObject();
                {
                    json.writeNumberField("timestamp", snapshot.getTimestamp().getTime());
                    json.writeNumberField("totalPending", snapshot.getTotalJobsPending());
                    json.writeNumberField("totalProcessed", snapshot.getTotalJobsProcessed());
                    json.writeNumberField("diffQueued", snapshot.getJobsQueuedSinceLastSnapshot());
                    json.writeNumberField("diffProcessed", snapshot.getJobsProcessedSinceLastSnapshot());
                    json.writeNumberField("heapUsed", snapshot.getHeapUsed());
                    json.writeNumberField("heapSize", snapshot.getHeapSize());
                }
                json.writeEndObject();
            }
            json.writeEndArray();
            json.writeFieldName("latest");
            json.writeStartObject();
            {
                json.writeNumberField("totalPending", jobQueueMetrics.getPendingJobsCount());
                json.writeNumberField("totalProcessed", jobQueueMetrics.getCompletedJobCount());
            }
            json.writeEndObject();
        }
        json.writeEndObject();
    }

    public void writeAllJobQueueStats(JsonGenerator json) throws IOException {

        ConcurrentHashMap<String, JobQueue> jobQueues = jobManager.getJobQueues();
        for(Map.Entry<String, JobQueue> entry : jobQueues.entrySet())
        {
            final String jobQueueName = entry.getKey();
            final JobQueue jobQueue = entry.getValue();

            if(jobQueue != null) {
                json.writeNumberField(jobQueueName, jobQueueMetrics.getPendingJobsCount(jobQueueName));
            }
        }
    }

    public void writeJobQueueSnapshots(String jobQueueName, JsonGenerator json,
                                       DateTime startTime,
                                       DateTime endTime) throws IOException
    {
        if(jobQueueMonitor != null)
        {
            ImmutableMap<String, JobQueueMetrics> snapshotMap = jobQueueMonitor.getSnapshots();
            if( snapshotMap != null && snapshotMap.containsKey(jobQueueName))
            {

                DateTime end = snapshotMap.get(jobQueueName).lowJobs.index().last();

                // If the last data point is before our end time, we need to use that data as the end
                if(end.isBefore(endTime)) {
                    endTime = end;
                }

                JobQueueMetrics jobQueueMetrics = snapshotMap.get(jobQueueName);
                TimeSeries high = jobQueueMetrics.highJobs.downSampleToTimeWindow(startTime,
                        endTime, Frequency.of(5, TimeUnit.MINUTES), SampleMethod.MEAN);
                TimeSeries mid = jobQueueMetrics.midJobs.downSampleToTimeWindow(startTime,
                        endTime, Frequency.of(5, TimeUnit.MINUTES), SampleMethod.MEAN);
                TimeSeries low = jobQueueMetrics.lowJobs.downSampleToTimeWindow(startTime,
                        endTime, Frequency.of(5, TimeUnit.MINUTES), SampleMethod.MEAN);

                json.writeFieldName("times");
                json.writeStartArray();
                for(DateTime t : high.index().asList()) {
                    json.writeNumber(t.toDate().getTime());
                }
                json.writeNumber(new Date().getTime());
                json.writeEndArray();

                json.writeFieldName("high");
                json.writeStartArray();
                for(BigDecimal d : high.values()) {
                    json.writeNumber(d);
                }
                json.writeNumber(jobQueueMetrics.highJobs.lastValue());
                json.writeEndArray();

                json.writeFieldName("mid");

                json.writeStartArray();
                for(BigDecimal d : mid.values()) {
                    json.writeNumber(d);
                }
                json.writeNumber(jobQueueMetrics.midJobs.lastValue());
                json.writeEndArray();

                json.writeFieldName("low");

                json.writeStartArray();
                for(BigDecimal d : low.values()) {
                    json.writeNumber(d);
                }
                json.writeNumber(jobQueueMetrics.lowJobs.lastValue());
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
