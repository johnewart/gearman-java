package net.johnewart.gearman.server.util;

import java.util.Date;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 12/23/12
 * Time: 10:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class JobQueueSnapshot {
    private Date timestamp;
    private long immediate;
    private long future;
    private HashMap<Integer, Long> futureJobCounts;
    private HashMap<String, Long> priorityCounts;

    public JobQueueSnapshot(Date timestamp, long immediate, long future, HashMap futureJobCounts, HashMap priorityCounts)
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

    public HashMap<Integer, Long> getFutureJobCounts() {
        return futureJobCounts;
    }

    public HashMap<String, Long> getPriorityCounts() {
        return priorityCounts;
    }
}
