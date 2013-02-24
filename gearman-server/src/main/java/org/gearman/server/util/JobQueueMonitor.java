package org.gearman.server.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import org.gearman.server.Job;
import org.gearman.server.JobQueue;
import org.gearman.server.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class JobQueueMonitor {
    private JobStore jobStore;
    private HashMap<String, List<JobQueueSnapshot>> snapshots;
    private final Logger LOG = LoggerFactory.getLogger(JobQueueMonitor.class);

    public JobQueueMonitor(JobStore jobStore)
    {
        this.jobStore = jobStore;
        this.snapshots = new HashMap<>();

        ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor();

        Runnable periodicTask = new Runnable() {
            public void run() {
                // Invoke method(s) to do the work
                snapshotJobQueues();
            }
        };

        executor.scheduleAtFixedRate(periodicTask, 0, 30, TimeUnit.SECONDS);

    }

    private void snapshotJobQueues()
    {
        LOG.debug("Snapshotting job queues.");
        // Time in seconds
        long currentTime = new Date().getTime() / 1000;
        for(String jobQueueName : jobStore.getJobQueues().keySet())
        {
            JobQueue jobQueue = jobStore.getJobQueues().get(jobQueueName);
            if(!snapshots.containsKey(jobQueueName))
            {
                snapshots.put(jobQueueName, new ArrayList<JobQueueSnapshot>());
            }

            HashMap<String, ImmutableList<Job>> copyOfQueues = jobQueue.getCopyOfJobQueues();
            HashMap<Integer, Long> hourCounts = new HashMap<>();
            HashMap<String, Long> priorityCounts = new HashMap<>();

            Long future = 0L;
            Long immediate = 0L;

            for(String priority : copyOfQueues.keySet())
            {
                ImmutableList<Job> queue = copyOfQueues.get(priority);
                priorityCounts.put(priority, new Long(queue.size()));

                for(Job job : queue)
                {
                    if(job.getTimeToRun() > 0)
                    {
                        Integer hoursFromNow = (int)((job.getTimeToRun() - currentTime) / 3600);

                        // Things in the past are present jobs
                        if(hoursFromNow < 0)
                        {
                            hoursFromNow = 0;
                        }

                        if(!hourCounts.containsKey(hoursFromNow))
                        {
                            hourCounts.put(hoursFromNow, 0L);
                        }

                        hourCounts.put(hoursFromNow, hourCounts.get(hoursFromNow) + 1);
                        future += 1;
                    } else {
                        immediate += 1;
                    }
                }
            }

            JobQueueSnapshot snapshot = new JobQueueSnapshot(new Date(), immediate, future, hourCounts, priorityCounts);

            snapshots.get(jobQueueName).add(snapshot);
        }
    }

    public JobStore getJobStore() {
        return jobStore;
    }

    public HashMap<String, List<JobQueueSnapshot>> getSnapshots() {
        return snapshots;
    }
}
