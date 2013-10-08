package net.johnewart.gearman.server.util;

import com.google.common.collect.ImmutableMap;
import net.johnewart.gearman.constants.JobPriority;

import java.util.Date;

public class JobQueueSnapshot {
    private Date timestamp;
    private long immediate;
    private long future;
    private ImmutableMap<Integer, Long> futureJobCounts;
    private ImmutableMap<JobPriority, Long> priorityCounts;

    public JobQueueSnapshot(Date timestamp,
                            long immediate,
                            long future,
                            ImmutableMap<Integer, Long> futureJobCounts,
                            ImmutableMap<JobPriority, Long> priorityCounts)
    {
        this.timestamp = timestamp;
        this.immediate = immediate;
        this.future = future;
        this.futureJobCounts = futureJobCounts;
        this.priorityCounts = priorityCounts;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public long getImmediate() {
        return immediate;
    }

    public long getFuture() {
        return future;
    }

    public ImmutableMap<Integer, Long> getFutureJobCounts() {
        return futureJobCounts;
    }

    public ImmutableMap<JobPriority, Long> getPriorityCounts() {
        return priorityCounts;
    }
}
