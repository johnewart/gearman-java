package org.gearman.server.util;

import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 12/23/12
 * Time: 10:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class JobQueueSnapshot {
    private Date timestamp;
    private long count;

    public JobQueueSnapshot(Date timestamp, long count)
    {
        this.timestamp = timestamp;
        this.count = count;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public long getCount() {
        return count;
    }
}
