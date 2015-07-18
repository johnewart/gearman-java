package net.johnewart.gearman.engine.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.interfaces.EngineWorker;
import org.joda.time.DateTime;

import java.util.concurrent.ConcurrentHashMap;

import static com.codahale.metrics.MetricRegistry.name;

public class MetricsEngine implements QueueMetrics {
    private final ConcurrentHashMap<String, CounterGroup> queueCounters = new ConcurrentHashMap<>();

    private final MetricRegistry registry;
    private final Counter pendingJobsCounter;
    private final Counter queuedJobsCounter;
    private final Counter completedJobsCounter;
    private final Counter failedJobsCounter;
    private final Counter activeJobsCounter;
    private final Counter jobExceptionsCounter;
    private final Meter jobMeter;
    private final Counter activeWorkersCounter;

    private final DateTime startTime;

    public MetricsEngine(MetricRegistry registry) {
        this.startTime = DateTime.now();
        this.registry = registry;
        pendingJobsCounter = registry.counter(name(MetricsEngine.class, "pending-jobs"));
        queuedJobsCounter = registry.counter(name(MetricsEngine.class, "queued-jobs"));
        completedJobsCounter = registry.counter(name(MetricsEngine.class, "completed-jobs"));
        failedJobsCounter = registry.counter(name(MetricsEngine.class, "failed-jobs"));
        activeJobsCounter = registry.counter(name(MetricsEngine.class, "active-jobs"));
        jobExceptionsCounter = registry.counter(name(MetricsEngine.class, "job-exceptions"));
        jobMeter = registry.meter(name(MetricsEngine.class, "queued-jobs-meter", "enqueued"));
        activeWorkersCounter = registry.counter(name(MetricsEngine.class, "active-workers"));
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
        return activeJobsCounter.getCount();
    }

    @Override
    public long getActiveJobCount(String queueName) {
        return countersForQueue(queueName).active.getCount();
    }

    @Override
    public long getEnqueuedJobCount() {
        return queuedJobsCounter.getCount();
    }

    @Override
    public long getEnqueuedJobCount(String queueName) {
        return countersForQueue(queueName).queued.getCount();
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
        return completedJobsCounter.getCount();
    }

    @Override
    public long getCompletedJobCount(String queueName) {
        return countersForQueue(queueName).completed.getCount();
    }

    @Override
    public long getFailedJobCount() {
        return failedJobsCounter.getCount();
    }

    @Override
    public long getFailedJobCount(String queueName) {
        return countersForQueue(queueName).failed.getCount();
    }

    @Override
    public long getExceptionCount() {
        return jobExceptionsCounter.getCount();
    }

    @Override
    public long getExceptionCount(String queueName) {
        return countersForQueue(queueName).exceptions.getCount();
    }

    @Override
    public long getRunningJobsCount() {
        return activeJobsCounter.getCount();
    }

    @Override
    public long getRunningJobsCount(String queueName) {
        return countersForQueue(queueName).active.getCount();
    }

    @Override
    public long getPendingJobsCount() {
        return pendingJobsCounter.getCount();
    }

    @Override
    public long getPendingJobsCount(String queueName) {
        return countersForQueue(queueName).low.getCount() +
               countersForQueue(queueName).mid.getCount() +
               countersForQueue(queueName).high.getCount();
    }

    @Override
    public long getHighPriorityJobsCount(String queueName) {
        return countersForQueue(queueName).high.getCount();
    }

    @Override
    public long getMidPriorityJobsCount(String queueName) {
        return countersForQueue(queueName).mid.getCount();
    }

    @Override
    public long getLowPriorityJobsCount(String queueName) {
        return countersForQueue(queueName).low.getCount();
    }

    @Override
    public ImmutableList<String> getQueueNames() {
        return ImmutableList.copyOf(queueCounters.keySet());
    }

    @Override
    public long getActiveWorkers() {
        return activeWorkersCounter.getCount();
    }

    @Override
    public long getActiveWorkers(String queueName) {
        return countersForQueue(queueName).workers.getCount();
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
            queueCounters.put(queueName, new CounterGroup(queueName, registry));
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

        public CounterGroup(String queueName, MetricRegistry registry) {
            low = registry.counter(name(MetricsEngine.class, queueName, "pending.low"));
            mid = registry.counter(name(MetricsEngine.class, queueName, "pending.mid"));
            high = registry.counter(name(MetricsEngine.class, queueName, "pending.high"));
            queued = registry.counter(name(MetricsEngine.class, queueName, "queued"));
            completed = registry.counter(name(MetricsEngine.class, queueName, "completed"));
            failed = registry.counter(name(MetricsEngine.class, queueName, "failed"));
            active = registry.counter(name(MetricsEngine.class, queueName, "active"));
            exceptions = registry.counter(name(MetricsEngine.class, queueName, "exceptions"));
            workers = registry.counter(name(MetricsEngine.class, queueName, "workers"));
        }
    }
}
