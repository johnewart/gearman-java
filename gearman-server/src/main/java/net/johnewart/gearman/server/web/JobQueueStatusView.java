package net.johnewart.gearman.server.web;

import java.util.List;

import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.server.util.JobQueueMonitor;
import net.johnewart.gearman.server.util.JobQueueSnapshot;

public class JobQueueStatusView extends StatusView {
    private final String jobQueueName;

    public JobQueueStatusView(final JobQueueMonitor jobQueueMonitor,
                              final JobManager jobManager,
                              final String jobQueueName)
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
        if (snapshots.size() > 0) {
            return snapshots.get(snapshots.size()-1);
        } else {
            return new JobQueueSnapshot();
        }
    }

    public Long getNumberOfConnectedWorkers()
    {
        return jobManager.getWorkerPool(jobQueueName).getNumberOfConnectedWorkers();
    }
}
