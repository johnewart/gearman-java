package org.gearman.server.util;

import com.google.common.collect.ImmutableSet;
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

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 12/23/12
 * Time: 10:22 PM
 * To change this template use File | Settings | File Templates.
 */
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
        for(String jobQueueName : jobStore.getJobQueues().keySet())
        {
            JobQueue jobQueue = jobStore.getJobQueues().get(jobQueueName);
            if(!snapshots.containsKey(jobQueueName))
            {
                snapshots.put(jobQueueName, new ArrayList<JobQueueSnapshot>());
            } else {
                JobQueueSnapshot snapshot = new JobQueueSnapshot(new Date(), jobQueue.size());
                snapshots.get(jobQueueName).add(snapshot);
            }
        }
    }

    public JobStore getJobStore() {
        return jobStore;
    }

    public HashMap<String, List<JobQueueSnapshot>> getSnapshots() {
        return snapshots;
    }
}
