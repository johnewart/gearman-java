package net.johnewart.gearman.server.util;

import com.google.common.collect.ImmutableList;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import net.johnewart.gearman.server.storage.JobManager;
import net.johnewart.gearman.server.storage.JobQueue;
import net.johnewart.gearman.server.core.QueuedJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class JobQueueMonitor {
    private JobManager jobManager;
    private HashMap<String, List<JobQueueSnapshot>> snapshots;
    private final Logger LOG = LoggerFactory.getLogger(JobQueueMonitor.class);
    private List<SystemSnapshot> systemSnapshots;

    public JobQueueMonitor(JobManager jobManager)
    {
        this.jobManager = jobManager;
        this.snapshots = new HashMap<>();
        this.systemSnapshots = new LinkedList<>();

        ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor();

        Runnable periodicTask = new Runnable() {
            public void run() {
                final Timer timer = Metrics.newTimer(JobQueueMonitor.class, "snapshots", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

                final TimerContext context = timer.time();

                try {
                    snapshotJobQueues();
                    condenseDataPoints();
                } finally {
                    context.stop();
                }
            }
        };

        executor.scheduleAtFixedRate(periodicTask, 0, 30, TimeUnit.SECONDS);

    }

    private void condenseDataPoints() {
        LOG.debug("Condensing data points");

    }

    private void snapshotJobQueues()
    {
        LOG.debug("Snapshotting job queues.");
        // Time in seconds
        long currentTime = new Date().getTime() / 1000;

        for(String jobQueueName : jobManager.getJobQueues().keySet())
        {
            JobQueue jobQueue = jobManager.getJobQueues().get(jobQueueName);
            if(!snapshots.containsKey(jobQueueName))
            {
                snapshots.put(jobQueueName, new LinkedList<JobQueueSnapshot>());
            }

            HashMap<String, ImmutableList<QueuedJob>> copyOfQueues = jobQueue.getCopyOfJobQueues();
            HashMap<Integer, Long> hourCounts = new HashMap<>();
            HashMap<String, Long> priorityCounts = new HashMap<>();

            Long futureCount = 0L;
            Long immediateCount = 0L;

            for(String priority : copyOfQueues.keySet())
            {
                ImmutableList<QueuedJob> queue = copyOfQueues.get(priority);
                priorityCounts.put(priority, new Long(queue.size()));

                for(QueuedJob job : queue)
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
        Long totalProcessed = jobManager.getCompletedJobsCounter().count();
        Long totalQueued = jobManager.getQueuedJobsCounter().count();
        Long totalPending = jobManager.getPendingJobsCounter().count();
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
