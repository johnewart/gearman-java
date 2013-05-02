package org.gearman.server.web;

import org.gearman.server.JobQueue;
import org.gearman.server.JobStore;
import org.gearman.server.util.JobQueueMonitor;
import org.gearman.server.util.JobQueueSnapshot;
import org.gearman.server.util.SystemSnapshot;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 4/30/13
 * Time: 6:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class StatusView {
    protected final JobQueueMonitor jobQueueMonitor;
    protected final JobStore jobStore;

    public StatusView(JobQueueMonitor jobQueueMonitor, JobStore jobStore)
    {
        this.jobQueueMonitor = jobQueueMonitor;
        this.jobStore = jobStore;
    }

    public List<JobQueue> getJobQueues()
    {
        return new ArrayList<>(jobStore.getJobQueues().values());
    }

    public long getUptimeInSeconds()
    {
        Date now = new Date();
        Date startTime = JobStore.timeStarted;
        long uptimeMilliseconds = now.getTime() - JobStore.timeStarted.getTime();
        long uptimeSeconds = uptimeMilliseconds / 1000;

        return uptimeSeconds;
    }

    public Integer getUptimeInDays()
    {
        return new Long(getUptimeInSeconds() / 86400).intValue();

    }

    public String getUptime()
    {
        TimeMap timeMap = DateFormatter.buildTimeMap(this.getUptimeInSeconds() * 1000);

        String res = "";

        if (timeMap.DAYS == 0) {
            if(timeMap.HOURS == 0)
                res = String.format("%dmin.", timeMap.MINUTES);
            else
                res = String.format("%dhrs.", timeMap.HOURS);
        } else if (timeMap.YEARS == 0) {
            res = String.format("%ddays", timeMap.DAYS);
        } else {
            res = String.format("> 1yr.", timeMap.YEARS);
        }

        return res;
    }

    public String getPersistenceEngineInfo()
    {
        return jobStore.getPersistenceEngine().getIdentifier();
    }

    public Long getTotalJobsPending()
    {
        long total = 0;
        for(JobQueue jobQueue : getJobQueues())
        {
            total += jobQueue.size();
        }

        return total;
    }

    public Long getTotalJobsQueued()
    {
        return jobStore.getQueuedJobs().count();
    }

    public Long getTotalJobsProcessed()
    {
        return jobStore.getCompletedJobs().count();
    }

    public Integer getWorkerCount()
    {
        return jobStore.getWorkerCount();
    }

    public String getHostname()
    {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return "nohostname";
        }

    }

    public boolean getMonitorEnabled()
    {
        return jobQueueMonitor != null;
    }

    public List<SystemSnapshot> getSystemSnapshots()
    {
        return jobQueueMonitor.getSystemSnapshots();
    }

    public SystemSnapshot getLatestSystemSnapshot()
    {
        List<SystemSnapshot> snapshots = getSystemSnapshots();
        return snapshots.get(snapshots.size()-1);
    }

    public List<JobQueueSnapshot> getJobQueueSnapshots(String jobQueueName)
    {
        Map<String, List<JobQueueSnapshot>> snapshotMap = jobQueueMonitor.getSnapshots();
        if(snapshotMap.containsKey(jobQueueName))
        {
            return snapshotMap.get(jobQueueName);
        } else {
            return new ArrayList<>();
        }
    }

    public long getMaxMemory()
    {
        return Runtime.getRuntime().maxMemory() / (1024 * 1024);
    }

    public Long getUsedMemory()
    {
        return Runtime.getRuntime().totalMemory() / (1024 * 1024);
    }

    public Integer getMemoryUsage()
    {
        return new Float((new Float(getUsedMemory()) / new Float(getMaxMemory())) * 100).intValue();
    }

    public NumberFormatter getNumberFormatter()
    {
        return new NumberFormatter();
    }

}

