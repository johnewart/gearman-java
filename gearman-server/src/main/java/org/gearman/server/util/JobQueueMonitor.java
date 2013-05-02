package org.gearman.server.util;

import com.google.common.collect.ImmutableList;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.gearman.server.JobQueue;
import org.gearman.server.JobStore;
import org.gearman.server.core.RunnableJob;
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
    private List<SystemSnapshot> systemSnapshots;

    public JobQueueMonitor(JobStore jobStore)
    {
        this.jobStore = jobStore;
        this.snapshots = new HashMap<>();
        this.systemSnapshots = new ArrayList<>();

        ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor();

        Runnable periodicTask = new Runnable() {
            public void run() {
                final Timer timer = Metrics.newTimer(JobQueueMonitor.class, "snapshots", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

                final TimerContext context = timer.time();

                try {
                    snapshotJobQueues();
                } finally {
                    context.stop();
                }
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

            HashMap<String, ImmutableList<RunnableJob>> copyOfQueues = jobQueue.getCopyOfJobQueues();
            HashMap<Integer, Long> hourCounts = new HashMap<>();
            HashMap<String, Long> priorityCounts = new HashMap<>();

            Long futureCount = 0L;
            Long immediateCount = 0L;

            for(String priority : copyOfQueues.keySet())
            {
                ImmutableList<RunnableJob> queue = copyOfQueues.get(priority);
                priorityCounts.put(priority, new Long(queue.size()));

                for(RunnableJob job : queue)
                {
                    long timeDiff = job.timeToRun - currentTime;

                    if(timeDiff > 0)
                    {
                        Integer hoursFromNow = (int)(timeDiff / 3600);

                        if(!hourCounts.containsKey(hoursFromNow))
                        {
                            hourCounts.put(hoursFromNow, 0L);
                        }

                        hourCounts.put(hoursFromNow, hourCounts.get(hoursFromNow) + 1);
                        futureCount += 1;
                    } else {
                        immediateCount += 1;
                    }
                }
            }

            JobQueueSnapshot snapshot = new JobQueueSnapshot(new Date(), immediateCount, futureCount, hourCounts, priorityCounts);
            snapshots.get(jobQueueName).add(snapshot);
        }


        // Generate an overall snapshot
        SystemSnapshot currentSnapshot;
        Long totalProcessed = jobStore.getCompletedJobs().count();
        Long totalQueued = jobStore.getQueuedJobs().count();
        Long totalPending = jobStore.getPendingJobs().count();
        Long heapUsed = Runtime.getRuntime().totalMemory();
        if(systemSnapshots.size() > 0)
        {
            SystemSnapshot previousSnapshot = systemSnapshots.get(systemSnapshots.size()-1);
            long processedDiff = totalProcessed - previousSnapshot.getTotalJobsProcessed();
            long queuedDiff = totalQueued - previousSnapshot.getTotalJobsQueued();
            currentSnapshot = new SystemSnapshot(totalQueued, totalProcessed, queuedDiff, processedDiff, totalPending, heapUsed);
        } else {
            currentSnapshot = new SystemSnapshot(totalQueued, totalProcessed, 0L, 0L, totalPending, heapUsed);
        }

        systemSnapshots.add(currentSnapshot);
    }

    public HashMap<String, List<JobQueueSnapshot>> getSnapshots() {
        return snapshots;
    }

    public List<SystemSnapshot> getSystemSnapshots() {
        return systemSnapshots;
    }
}
