package net.johnewart.gearman.server.web;

import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.engine.queue.JobQueue;
import net.johnewart.gearman.server.util.JobQueueMonitor;
import net.johnewart.gearman.server.util.JobQueueSnapshot;
import net.johnewart.gearman.server.util.SystemSnapshot;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class StatusView {
    protected final JobQueueMonitor jobQueueMonitor;
    protected final JobManager jobManager;

    public StatusView(JobQueueMonitor jobQueueMonitor, JobManager jobManager)
    {
        this.jobQueueMonitor = jobQueueMonitor;
        this.jobManager = jobManager;
    }

    public List<JobQueue> getJobQueues()
    {
        return new ArrayList<>(jobManager.getJobQueues().values());
    }

    public long getUptimeInSeconds()
    {
        Date now = new Date();
        return now.getTime() - JobManager.timeStarted.getTime() / 1000;
    }

    public Integer getUptimeInDays()
    {
        return (Long.valueOf(getUptimeInSeconds() / 86400)).intValue();

    }

    public String getUptime()
    {
        TimeMap timeMap = DateFormatter.buildTimeMap(this.getUptimeInSeconds() * 1000);

        String res;

        if (timeMap.DAYS == 0) {
            if(timeMap.HOURS == 0)
                res = String.format("%dmin.", timeMap.MINUTES);
            else
                res = String.format("%dhrs.", timeMap.HOURS);
        } else if (timeMap.YEARS == 0) {
            res = String.format("%ddays", timeMap.DAYS);
        } else {
            res = "> 1yr.";
        }

        return res;
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
        return jobManager.getQueuedJobsCounter().count();
    }

    public Long getTotalJobsProcessed()
    {
        return jobManager.getCompletedJobsCounter().count();
    }

    public Integer getWorkerCount()
    {
        return jobManager.getWorkerCount();
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
        return new Float(((float) getUsedMemory() / (float) getMaxMemory()) * 100).intValue();
    }

    public NumberFormatter getNumberFormatter()
    {
        return new NumberFormatter();
    }

}

