package net.johnewart.gearman.server.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;


public class NoopJobQueueMonitor implements JobQueueMonitor {

    @Override
    public ImmutableMap<String, JobQueueMetrics> getSnapshots() {
        return ImmutableMap.of();
    }

    @Override
    public ImmutableList<SystemSnapshot> getSystemSnapshots() {
        return ImmutableList.of();
    }
}
