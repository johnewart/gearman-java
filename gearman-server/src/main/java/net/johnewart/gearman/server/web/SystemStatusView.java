package net.johnewart.gearman.server.web;

import net.johnewart.gearman.server.storage.JobManager;
import net.johnewart.gearman.server.util.JobQueueMonitor;

public class SystemStatusView extends StatusView {

    public SystemStatusView(JobQueueMonitor jobQueueMonitor, JobManager jobManager)
    {
        super(jobQueueMonitor, jobManager);
    }
}
