package net.johnewart.gearman.engine.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.interfaces.EngineWorker;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.engine.queue.JobQueue;
import org.joda.time.DateTime;

import java.util.concurrent.ConcurrentHashMap;

import static com.codahale.metrics.MetricRegistry.name;

public class MetricsEngine implements QueueMetrics {
    private final ConcurrentHashMap<String, CounterGroup> queueCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> workerCounters = new ConcurrentHashMap<>();

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
        pendingJobsCounter = registry.counter(name("queues", "pending-jobs"));
        queuedJobsCounter = registry.counter(name("queues", "queued-jobs"));
        completedJobsCounter = registry.counter(name("queues", "completed-jobs"));
        failedJobsCounter = registry.counter(name("queues", "failed-jobs"));
        activeJobsCounter = registry.counter(name("queues", "active-jobs"));
        jobExceptionsCounter = registry.counter(name("queues", "job-exceptions"));
        jobMeter = registry.meter(name("queues", "queued-jobs-meter", "enqueued"));
        activeWorkersCounter = registry.counter(name("queues", "active-workers"));
    }

    @Override
    public void handleJobCompleted(Job job) {
        completedJobsCounter.inc();
        queueCounters.get(job.getFunctionName()).completed.inc();
        decrementActive(job);
    }

    @Override
    public void handleJobFailed(Job job) {
        failedJobsCounter.inc();
        queueCounters.get(job.getFunctionName()).failed.inc();
        decrementActive(job);
    }

    @Override
    public void handleJobEnqueued(Job job) {
        jobMeter.mark();
        queuedJobsCounter.inc();
        queueCounters.get(job.getFunctionName()).queued.inc();
        incrementPending();
    }

    @Override
    public void handleWorkerAddition(EngineWorker worker) {
        activeWorkersCounter.inc();
        for(String jobQueue : worker.getAbilities()) {
            workerCounters.getOrDefault(jobQueue, registry.counter(name("queue", jobQueue, "workers"))).inc();
        }
    }

    @Override
    public void handleWorkerRemoval(EngineWorker worker) {
        activeWorkersCounter.dec();
        for(String jobQueue : worker.getAbilities()) {
            workerCounters.get(jobQueue).dec();
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
        return queueCounters.get(queueName).active.getCount();
    }

    @Override
    public long getEnqueuedJobCount() {
        return queuedJobsCounter.getCount();
    }

    @Override
    public long getEnqueuedJobCount(String queueName) {
        return queueCounters.get(queueName).queued.getCount();
    }

    @Override
    public void handleJobStarted(Job job) {
        decrementPending();
        incrementActive(job);
        queueCounters.get(job.getFunctionName()).active.inc();
    }

    @Override
    public void handleJobException(Job job) {
        decrementActive(job);
        jobExceptionsCounter.inc();
        queueCounters.get(job.getFunctionName()).exceptions.inc();
    }

    @Override
    public long getCompletedJobCount() {
        return completedJobsCounter.getCount();
    }

    @Override
    public long getCompletedJobCount(String queueName) {
        return queueCounters.get(queueName).completed.getCount();
    }

    @Override
    public long getFailedJobCount() {
        return failedJobsCounter.getCount();
    }

    @Override
    public long getFailedJobCount(String queueName) {
        return queueCounters.get(queueName).failed.getCount();
    }

    @Override
    public long getExceptionCount() {
        return jobExceptionsCounter.getCount();
    }

    @Override
    public long getExceptionCount(String queueName) {
        return queueCounters.get(queueName).exceptions.getCount();
    }

    @Override
    public long getRunningJobsCount() {
        return activeJobsCounter.getCount();
    }

    @Override
    public long getRunningJobsCount(String queueName) {
        return queueCounters.get(queueName).active.getCount();
    }

    @Override
    public long getPendingJobsCount() {
        return queueCounters
                .values()
                .stream()
                .mapToLong(counterGroup -> counterGroup.total.getValue())
                .sum();
    }

    @Override
    public long getPendingJobsCount(String queueName) {
        return queueCounters.get(queueName).low.getValue() +
               queueCounters.get(queueName).mid.getValue() +
               queueCounters.get(queueName).high.getValue();
    }

    @Override
    public long getHighPriorityJobsCount(String queueName) {
        return queueCounters.get(queueName).high.getValue();
    }

    @Override
    public long getMidPriorityJobsCount(String queueName) {
        return queueCounters.get(queueName).mid.getValue();
    }

    @Override
    public long getLowPriorityJobsCount(String queueName) {
        return queueCounters.get(queueName).low.getValue();
    }

    @Override
    public ImmutableList<String> getQueueNames() {
        return ImmutableList.copyOf(queueCounters.keySet());
    }

    @Override
    public long getActiveWorkers() {
        return activeWorkersCounter.getCount();
    }

    public long getActiveWorkers(String queueName) {
        return workerCounters.getOrDefault(queueName, registry.counter(name("queue", queueName, "workers"))).getCount();
    }

    @Override
    public void registerJobQueue(JobQueue jobQueue)
    {
        queueCounters.put(jobQueue.getName(), new CounterGroup(jobQueue, registry));
    }

    private void decrementActive(Job job) {
        activeJobsCounter.dec();
        queueCounters.get(job.getFunctionName()).active.dec();
    }

    private void incrementActive(Job job) {
        activeJobsCounter.inc();
        queueCounters.get(job.getFunctionName()).active.inc();
    }

    private void decrementPending() {
        pendingJobsCounter.dec();
    }

    private void incrementPending() {
        pendingJobsCounter.inc();
    }

    class CounterGroup {
        public final Counter queued;
        public final Counter completed;
        public final Counter failed;
        public final Counter active;
        public final Counter exceptions;
        public final Gauge<Integer> low;
        public final Gauge<Integer> mid;
        public final Gauge<Integer> high;
        public final Gauge<Integer> total;
        private final JobQueue jobQueue;

        public CounterGroup(JobQueue jobQueue, MetricRegistry registry) {
            this.jobQueue = jobQueue;
            final String queueName = jobQueue.getName();

            low = () -> this.jobQueue.size(JobPriority.LOW);
            mid = () -> this.jobQueue.size(JobPriority.NORMAL);
            high = () -> this.jobQueue.size(JobPriority.HIGH);
            total = () -> this.jobQueue.size();

            queued = registry.counter(name("queue", queueName, "queued"));
            completed = registry.counter(name("queue", queueName, "completed"));
            failed = registry.counter(name("queue", queueName, "failed"));
            active = registry.counter(name("queue", queueName, "active"));
            exceptions = registry.counter(name("queue", queueName, "exceptions"));
        }
    }
}
