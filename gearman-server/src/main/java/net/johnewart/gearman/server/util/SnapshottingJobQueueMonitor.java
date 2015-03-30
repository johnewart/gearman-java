package net.johnewart.gearman.server.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import net.johnewart.gearman.engine.metrics.QueueMetrics;
import net.johnewart.shuzai.Frequency;
import net.johnewart.shuzai.SampleMethod;
import net.johnewart.shuzai.TimeSeries;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SnapshottingJobQueueMonitor implements JobQueueMonitor {
    private HashMap<String, JobQueueMetrics> snapshots;
    private final Logger LOG = LoggerFactory.getLogger(SnapshottingJobQueueMonitor.class);
    private List<SystemSnapshot> systemSnapshots;
    private final int maxSnapshots = 2880;
    private final QueueMetrics queueMetrics;
    private final Object lockObject = new Object();

    public SnapshottingJobQueueMonitor(QueueMetrics queueMetrics)
    {
        this.snapshots = new HashMap<>();
        this.systemSnapshots = new LinkedList<>();
        this.queueMetrics = queueMetrics;

        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(2);

        Runnable snapshotTask = new Runnable() {
            public void run() {
                final Timer timer = Metrics.newTimer(SnapshottingJobQueueMonitor.class, "snapshots", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

                final TimerContext context = timer.time();

                try {
                    snapshotJobQueues();
                } finally {
                    context.stop();
                }
            }
        };

        Runnable compactTask = new Runnable() {
            @Override
            public void run() {
                condenseDataPoints();
            }
        };

        executor.scheduleAtFixedRate(compactTask, 2, 2, TimeUnit.MINUTES);
        executor.scheduleAtFixedRate(snapshotTask, 0, 5, TimeUnit.SECONDS);

    }

    private void condenseDataPoints() {
        synchronized(lockObject) {
            LOG.debug("Condensing data points");

            for (String jobQueueName : queueMetrics.getQueueNames()) {

                JobQueueMetrics metrics = snapshots.get(jobQueueName);

                if (metrics != null) {
                    try {
                        snapshots.put(jobQueueName, metrics.compact());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private void snapshotJobQueues()
    {
        synchronized (lockObject) {
            LOG.debug("Snapshotting job queues.");

            for (String jobQueueName : queueMetrics.getQueueNames()) {
                if (!snapshots.containsKey(jobQueueName)) {
                    snapshots.put(jobQueueName, new JobQueueMetrics());
                }

                JobQueueMetrics metrics = snapshots.get(jobQueueName);

                DateTime now = DateTime.now();
                Long highJobs = queueMetrics.getHighPriorityJobsCount(jobQueueName);
                Long midJobs = queueMetrics.getMidPriorityJobsCount(jobQueueName);
                Long lowJobs = queueMetrics.getLowPriorityJobsCount(jobQueueName);

                metrics.highJobs.add(now, highJobs);
                metrics.midJobs.add(now, midJobs);
                metrics.lowJobs.add(now, lowJobs);
                metrics.queued.add(now, queueMetrics.getEnqueuedJobCount(jobQueueName));
                metrics.exceptions.add(now, queueMetrics.getExceptionCount(jobQueueName));
                metrics.completed.add(now, queueMetrics.getCompletedJobCount(jobQueueName));
                metrics.failed.add(now, queueMetrics.getFailedJobCount(jobQueueName));

            }
        }

        // Generate an overall snapshot
        SystemSnapshot currentSnapshot;
        Long totalProcessed = queueMetrics.getCompletedJobCount();
        Long totalQueued = queueMetrics.getEnqueuedJobCount();
        Long totalPending = queueMetrics.getPendingJobsCount();
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

    private TimeSeries fiveMinuteAverage(TimeSeries original) {
        return original.downSample(Frequency.of(5, TimeUnit.MINUTES), SampleMethod.MEAN);
    }

    public ImmutableMap<String, JobQueueMetrics> getSnapshots() {
        return ImmutableMap.copyOf(snapshots);
    }

    public ImmutableList<SystemSnapshot> getSystemSnapshots() {
        return ImmutableList.copyOf(systemSnapshots);
    }
}
