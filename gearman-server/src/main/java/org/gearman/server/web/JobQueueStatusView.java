package org.gearman.server.web;

import org.gearman.server.storage.JobManager;
import org.gearman.server.util.JobQueueMonitor;
import org.gearman.server.util.JobQueueSnapshot;

import java.util.List;

public class JobQueueStatusView extends StatusView {
    private final String jobQueueName;

    public JobQueueStatusView(JobQueueMonitor jobQueueMonitor, JobManager jobManager, String jobQueueName)
    {
        super(jobQueueMonitor, jobManager);
        this.jobQueueName = jobQueueName;
    }

    public String getJobQueueName() {
        return jobQueueName;
    }

    public List<JobQueueSnapshot> getJobQueueSnapshots()
    {
        return this.getJobQueueSnapshots(this.jobQueueName);
    }

    public JobQueueSnapshot getLatestJobQueueSnapshot()
    {
        List<JobQueueSnapshot> snapshots = getJobQueueSnapshots();
        return snapshots.get(snapshots.size()-1);
    }

    public Integer getNumberOfConnectedWorkers()
    {
        return jobManager.getJobQueue(jobQueueName).getNumberOfConnectedWorkers();
    }
}
