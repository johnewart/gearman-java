package org.gearman.server.web;

import org.gearman.server.storage.JobStore;
import org.gearman.server.util.JobQueueMonitor;

public class SystemStatusView extends StatusView {

    public SystemStatusView(JobQueueMonitor jobQueueMonitor, JobStore jobStore)
    {
        super(jobQueueMonitor, jobStore);
    }
}
