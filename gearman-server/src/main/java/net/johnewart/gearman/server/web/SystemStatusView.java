package net.johnewart.gearman.server.web;

import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.server.util.SnapshottingJobQueueMonitor;

public class SystemStatusView extends StatusView {

    public SystemStatusView(SnapshottingJobQueueMonitor jobQueueMonitor, JobManager jobManager)
    {
        super(jobQueueMonitor, jobManager);
    }
}
