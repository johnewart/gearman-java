package net.johnewart.gearman.server.web;

import net.johnewart.gearman.engine.metrics.QueueMetrics;
import net.johnewart.gearman.server.util.SnapshottingJobQueueMonitor;

public class SystemStatusView extends StatusView {

    public SystemStatusView(SnapshottingJobQueueMonitor jobQueueMonitor, QueueMetrics queueMetrics)
    {
        super(jobQueueMonitor, queueMetrics);
    }
}
