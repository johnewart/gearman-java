package net.johnewart.gearman.engine.metrics;

import com.google.common.collect.ImmutableList;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.interfaces.EngineWorker;
import org.joda.time.DateTime;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class MetricsEngine implements QueueMetrics {
    private final Counter pendingJobsCounter = Metrics.newCounter(MetricsEngine.class, "pending-jobs");
    private final Counter queuedJobsCounter = Metrics.newCounter(MetricsEngine.class, "queued-jobs");
    private final Counter completedJobsCounter = Metrics.newCounter(MetricsEngine.class, "completed-jobs");
    private final Counter failedJobsCounter = Metrics.newCounter(MetricsEngine.class, "failed-jobs");
    private final Counter activeJobsCounter = Metrics.newCounter(MetricsEngine.class, "active-jobs");
    private final Counter jobExceptionsCounter = Metrics.newCounter(MetricsEngine.class, "job-exceptions");
    private final Meter jobMeter = Metrics.newMeter(MetricsEngine.class, "queued-jobs-meter", "enqueued", TimeUnit.SECONDS);
    private final Counter activeWorkersCounter = Metrics.newCounter(MetricsEngine.class, "active-workers");
    private final ConcurrentHashMap<String, CounterGroup> queueCounters = new ConcurrentHashMap<>();

    private final DateTime startTime;

    public MetricsEngine() {
        this.startTime = DateTime.now();
    }

    @Override
    public void handleJobCompleted(Job job) {
        completedJobsCounter.inc();
        countersForQueue(job.getFunctionName()).completed.inc();
        decrementActive(job);
    }

    @Override
    public void handleJobFailed(Job job) {
        failedJobsCounter.inc();
        countersForQueue(job.getFunctionName()).failed.inc();
        decrementActive(job);
    }

    @Override
    public void handleJobEnqueued(Job job) {
        jobMeter.mark();
        queuedJobsCounter.inc();
        countersForQueue(job.getFunctionName()).queued.inc();
        incrementPending(job);
    }

    @Override
    public void handleWorkerAddition(EngineWorker worker) {
        activeWorkersCounter.inc();
        for(String jobQueue : worker.getAbilities()) {
            countersForQueue(jobQueue).workers.inc();
        }
    }

    @Override
    public void handleWorkerRemoval(EngineWorker worker) {
        activeWorkersCounter.dec();
        for(String jobQueue : worker.getAbilities()) {
            countersForQueue(jobQueue).workers.dec();
        }
    }

    @Override
    public DateTime getStartTime() {
        return startTime;
    }

    @Override
    public long getActiveJobCount() {
        return activeJobsCounter.count();
    }

    @Override
    public long getActiveJobCount(String queueName) {
        return countersForQueue(queueName).active.count();
    }

    @Override
    public long getEnqueuedJobCount() {
        return queuedJobsCounter.count();
    }

    @Override
    public long getEnqueuedJobCount(String queueName) {
        return countersForQueue(queueName).queued.count();
    }

    @Override
    public void handleJobStarted(Job job) {
        decrementPending(job);
        incrementActive(job);
    }

    @Override
    public void handleJobException(Job job) {
        decrementActive(job);
        jobExceptionsCounter.inc();
        countersForQueue(job.getFunctionName()).exceptions.inc();
    }

    @Override
    public long getCompletedJobCount() {
        return completedJobsCounter.count();
    }

    @Override
    public long getCompletedJobCount(String queueName) {
        return countersForQueue(queueName).completed.count();
    }

    @Override
    public long getFailedJobCount() {
        return failedJobsCounter.count();
    }

    @Override
    public long getFailedJobCount(String queueName) {
        return countersForQueue(queueName).failed.count();
    }

    @Override
    public long getExceptionCount() {
        return jobExceptionsCounter.count();
    }

    @Override
    public long getExceptionCount(String queueName) {
        return countersForQueue(queueName).exceptions.count();
    }

    @Override
    public long getRunningJobsCount() {
        return activeJobsCounter.count();
    }

    @Override
    public long getRunningJobsCount(String queueName) {
        return countersForQueue(queueName).active.count();
    }

    @Override
    public long getPendingJobsCount() {
        return pendingJobsCounter.count();
    }

    @Override
    public long getPendingJobsCount(String queueName) {
        return countersForQueue(queueName).low.count() +
               countersForQueue(queueName).mid.count() +
               countersForQueue(queueName).high.count();
    }

    @Override
    public long getHighPriorityJobsCount(String queueName) {
        return countersForQueue(queueName).high.count();
    }

    @Override
    public long getMidPriorityJobsCount(String queueName) {
        return countersForQueue(queueName).mid.count();
    }

    @Override
    public long getLowPriorityJobsCount(String queueName) {
        return countersForQueue(queueName).low.count();
    }

    @Override
    public ImmutableList<String> getQueueNames() {
        return ImmutableList.copyOf(queueCounters.keySet());
    }

    @Override
    public long getActiveWorkers() {
        return activeWorkersCounter.count();
    }

    @Override
    public long getActiveWorkers(String queueName) {
        return countersForQueue(queueName).workers.count();
    }

    private void decrementActive(Job job) {
        activeJobsCounter.dec();
        countersForQueue(job.getFunctionName()).active.dec();
    }

    private void incrementActive(Job job) {
        activeJobsCounter.inc();
        countersForQueue(job.getFunctionName()).active.inc();
    }

    private void decrementPending(Job job) {
        pendingJobsCounter.dec();

        switch(job.getPriority()) {
            case LOW:
                countersForQueue(job.getFunctionName()).low.dec();
                break;
            case NORMAL:
                countersForQueue(job.getFunctionName()).mid.dec();
                break;
            case HIGH:
                countersForQueue(job.getFunctionName()).high.dec();
                break;
        }
    }

    private void incrementPending(Job job) {
        pendingJobsCounter.inc();

        switch(job.getPriority()) {
            case LOW:
                countersForQueue(job.getFunctionName()).low.inc();
                break;
            case NORMAL:
                countersForQueue(job.getFunctionName()).mid.inc();
                break;
            case HIGH:
                countersForQueue(job.getFunctionName()).high.inc();
                break;
        }
    }

    private CounterGroup countersForQueue(String queueName) {
        if(!queueCounters.containsKey(queueName)) {
            queueCounters.put(queueName, new CounterGroup(queueName));
        }

        return queueCounters.get(queueName);
    }

    class CounterGroup {
        public final Counter queued;
        public final Counter completed;
        public final Counter failed;
        public final Counter active;
        public final Counter exceptions;
        public final Counter low;
        public final Counter mid;
        public final Counter high;
        public final Counter workers;

        public CounterGroup(String queueName) {
            low = Metrics.newCounter(MetricsEngine.class, queueName, "pending.low");
            mid = Metrics.newCounter(MetricsEngine.class, queueName, "pending.mid");
            high = Metrics.newCounter(MetricsEngine.class, queueName, "pending.high");
            queued = Metrics.newCounter(MetricsEngine.class, queueName, "queued");
            completed = Metrics.newCounter(MetricsEngine.class, queueName, "completed");
            failed = Metrics.newCounter(MetricsEngine.class, queueName, "failed");
            active = Metrics.newCounter(MetricsEngine.class, queueName, "active");
            exceptions = Metrics.newCounter(MetricsEngine.class, queueName, "exceptions");
            workers = Metrics.newCounter(MetricsEngine.class, queueName, "workers");
        }
    }
}
