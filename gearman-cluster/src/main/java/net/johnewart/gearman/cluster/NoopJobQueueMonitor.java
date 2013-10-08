package net.johnewart.gearman.cluster;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import net.johnewart.gearman.server.util.JobQueueMonitor;
import net.johnewart.gearman.server.util.JobQueueSnapshot;
import net.johnewart.gearman.server.util.SystemSnapshot;

import java.util.List;


public class NoopJobQueueMonitor implements JobQueueMonitor {

    @Override
    public ImmutableMap<String, List<JobQueueSnapshot>> getSnapshots() {
        return ImmutableMap.of();
    }

    @Override
    public ImmutableList<SystemSnapshot> getSystemSnapshots() {
        return ImmutableList.of();
    }
}
