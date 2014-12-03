package net.johnewart.gearman.server.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.engine.queue.JobQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SnapshottingJobQueueMonitor implements JobQueueMonitor {
    private JobManager jobManager;
    private HashMap<String, List<JobQueueSnapshot>> snapshots;
    private final Logger LOG = LoggerFactory.getLogger(SnapshottingJobQueueMonitor.class);
    private List<SystemSnapshot> systemSnapshots;
    private final int maxSnapshots = 2880;

    public SnapshottingJobQueueMonitor(JobManager jobManager)
    {
        this.jobManager = jobManager;
        this.snapshots = new HashMap<>();
        this.systemSnapshots = new LinkedList<>();

        ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor();

        Runnable periodicTask = new Runnable() {
            public void run() {
                final Timer timer = Metrics.newTimer(SnapshottingJobQueueMonitor.class, "snapshots", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

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

        for(String jobQueueName : jobManager.getJobQueues().keySet())
        {
            JobQueue jobQueue = jobManager.getJobQueues().get(jobQueueName);

            if(!snapshots.containsKey(jobQueueName))
            {
                snapshots.put(jobQueueName, new LinkedList<JobQueueSnapshot>());
            }

            final ImmutableMap<JobPriority, Long> priorityCounts = ImmutableMap.of(
                JobPriority.LOW,
                jobQueue.size(JobPriority.LOW),
                JobPriority.NORMAL,
                jobQueue.size(JobPriority.NORMAL),
                JobPriority.HIGH,
                jobQueue.size(JobPriority.HIGH)
            );
            final ImmutableMap<Integer, Long> hourCounts = jobQueue.futureCounts();

            final long futureCount = sumOfJobsOccurringInOrAfter(hourCounts, 1);
            final long immediateCount = hourCounts.get(0);

            JobQueueSnapshot snapshot = new JobQueueSnapshot(new Date(), immediateCount, futureCount, hourCounts, priorityCounts);
            List<JobQueueSnapshot> snapshotList = snapshots.get(jobQueueName);

            if(snapshotList.size() == maxSnapshots) {
                snapshotList.remove(maxSnapshots - 1);
            }

            snapshotList.add(snapshot);
        }


        // Generate an overall snapshot
        SystemSnapshot currentSnapshot;
        Long totalProcessed = jobManager.getCompletedJobsCounter().count();
        Long totalQueued = jobManager.getQueuedJobsCounter().count();
        Long totalPending = jobManager.getPendingJobsCounter().count();
        Long heapSize = Runtime.getRuntime().totalMemory();
        Long heapUsed = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if(systemSnapshots.size() > 0)
        {
            SystemSnapshot previousSnapshot = systemSnapshots.get(systemSnapshots.size()-1);
            long processedDiff = totalProcessed - previousSnapshot.getTotalJobsProcessed();
            long queuedDiff = totalQueued - previousSnapshot.getTotalJobsQueued();
            currentSnapshot = new SystemSnapshot(totalQueued, totalProcessed, queuedDiff, processedDiff, totalPending, heapSize, heapUsed);
        } else {
            currentSnapshot = new SystemSnapshot(totalQueued, totalProcessed, 0L, 0L, totalPending, heapSize, heapUsed);
        }

        if(systemSnapshots.size() == maxSnapshots) {
            systemSnapshots.remove(maxSnapshots - 1);
        }

        systemSnapshots.add(currentSnapshot);
    }

    private long sumOfJobsOccurringInOrAfter(ImmutableMap<Integer, Long> counts, int threshold) {
        long sum = 0L;

        for (int hoursFromNow : counts.keySet()) {
            if (hoursFromNow >= threshold) {
                sum += counts.get(hoursFromNow);
            }
        }

        return sum;
    }

    public ImmutableMap<String, List<JobQueueSnapshot>> getSnapshots() {
        return ImmutableMap.copyOf(snapshots);
    }

    public ImmutableList<SystemSnapshot> getSystemSnapshots() {
        return ImmutableList.copyOf(systemSnapshots);
    }
}
