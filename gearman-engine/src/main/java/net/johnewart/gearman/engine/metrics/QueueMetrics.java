package net.johnewart.gearman.engine.metrics;

import com.google.common.collect.ImmutableList;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.interfaces.EngineWorker;
import org.joda.time.DateTime;

import java.util.Collection;

public interface QueueMetrics {
    public void handleJobCompleted(Job job);
    public void handleJobFailed(Job job);
    public void handleJobStarted(Job job);
    public void handleJobException(Job job);
    public void handleJobEnqueued(Job job);
    public void handleWorkerAddition(EngineWorker worker);
    public void handleWorkerRemoval(EngineWorker worker);

    public DateTime getStartTime();

    public long getActiveJobCount();
    public long getActiveJobCount(String queueName);

    public long getEnqueuedJobCount();
    public long getEnqueuedJobCount(String queueName);

    public long getCompletedJobCount();
    public long getCompletedJobCount(String queueName);

    public long getFailedJobCount();
    public long getFailedJobCount(String queueName);

    public long getExceptionCount();
    public long getExceptionCount(String queueName);

    public long getRunningJobsCount();
    public long getRunningJobsCount(String queueName);

    public long getPendingJobsCount();
    public long getPendingJobsCount(String queueName);

    public long getHighPriorityJobsCount(String queueName);
    public long getMidPriorityJobsCount(String queueName);
    public long getLowPriorityJobsCount(String queueName);

    public ImmutableList<String> getQueueNames();

    public long getActiveWorkers();
    public long getActiveWorkers(String queueName);

}
