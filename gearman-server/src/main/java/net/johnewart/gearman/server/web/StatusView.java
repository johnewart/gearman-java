package net.johnewart.gearman.server.web;

import java.net.UnknownHostException;
import java.util.*;

import net.johnewart.gearman.engine.metrics.QueueMetrics;
import net.johnewart.gearman.server.util.JobQueueMetrics;
import net.johnewart.gearman.server.util.JobQueueMonitor;
import net.johnewart.gearman.server.util.SystemSnapshot;

public class StatusView {
    protected final JobQueueMonitor jobQueueMonitor;
    protected final QueueMetrics queueMetrics;

    public StatusView(JobQueueMonitor jobQueueMonitor, QueueMetrics queueMetrics)
    {
        this.jobQueueMonitor = jobQueueMonitor;
        this.queueMetrics = queueMetrics;
    }

    public List<String> getJobQueues()
    {
        List<String> queueNames = new ArrayList<>(queueMetrics.getQueueNames());
        queueNames.sort(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });

        return queueNames;
    }

    public int getJobQueueCount() {
        return queueMetrics.getQueueNames().size();
    }

    public long getUptimeInSeconds()
    {
        Date now = new Date();
        Date started = queueMetrics.getStartTime().toDate();
        return (now.getTime() - started.getTime()) / 1000;
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
        return queueMetrics.getPendingJobsCount();
    }

    public Long getTotalJobsQueued()
    {
        return queueMetrics.getEnqueuedJobCount();
    }

    public Long getTotalJobsProcessed()
    {
        return queueMetrics.getCompletedJobCount();
    }

    public Long getWorkerCount()
    {
        return queueMetrics.getActiveWorkers();
    }

    public Long getWorkerCount(String jobQueueName) {
        return queueMetrics.getActiveWorkers(jobQueueName);
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

    public JobQueueMetrics getJobQueueSnapshots(String jobQueueName)
    {
        Map<String, JobQueueMetrics> snapshotMap = jobQueueMonitor.getSnapshots();
        if(snapshotMap.containsKey(jobQueueName))
        {
            return snapshotMap.get(jobQueueName);
        } else {
            return new JobQueueMetrics();
        }
    }

    public long getMaxHeapSize()
    {
        return Runtime.getRuntime().maxMemory() / (1024 * 1024);
    }

    public Long getUsedMemory()
    {
        return Runtime.getRuntime().totalMemory() / (1024 * 1024);
    }

    public Long getHeapSize() { return Runtime.getRuntime().totalMemory() / (1024 * 1024); }

    public Long getHeapUsed() {
        return (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024);
    }

    public Integer getMemoryUsage()
    {
        return new Float(((float) getHeapUsed() / (float) getMaxHeapSize()) * 100).intValue();
    }

    public NumberFormatter getNumberFormatter()
    {
        return new NumberFormatter();
    }

}

