package org.gearman.server.web;

import org.gearman.server.storage.JobManager;
import org.gearman.server.util.JobQueueMonitor;

public class SystemStatusView extends StatusView {

    public SystemStatusView(JobQueueMonitor jobQueueMonitor, JobManager jobManager)
    {
        super(jobQueueMonitor, jobManager);
    }
}
