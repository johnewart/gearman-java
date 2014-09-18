package net.johnewart.gearman.engine.core;

import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.common.Job;

public final class QueuedJob implements Comparable<QueuedJob>
{
    public final String uniqueID;
    public final Long timeToRun;
    public final JobPriority priority;
    public final String functionName;

    public QueuedJob(String uniqueID, long timeToRun, JobPriority priority, String functionName)
    {
        this.uniqueID = uniqueID;
        this.timeToRun = timeToRun;
        this.priority = priority;
        this.functionName = functionName;
    }

    public QueuedJob(Job job)
    {
        this.uniqueID = job.getUniqueID();
        this.timeToRun = job.getTimeToRun();
        this.priority = job.getPriority();
        this.functionName = job.getFunctionName();
    }

    public String getUniqueID() {
        return uniqueID;
    }

    public Long getTimeToRun() {
        return timeToRun;
    }

    public JobPriority getPriority() {
        return priority;
    }

    public String getFunctionName() {
        return functionName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueuedJob that = (QueuedJob) o;

        if (functionName != null ? !functionName.equals(that.functionName) : that.functionName != null) return false;
        if (priority != that.priority) return false;
        if (timeToRun != null ? !timeToRun.equals(that.timeToRun) : that.timeToRun != null) return false;
        if (uniqueID != null ? !uniqueID.equals(that.uniqueID) : that.uniqueID != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = uniqueID != null ? uniqueID.hashCode() : 0;
        result = 31 * result + (timeToRun != null ? timeToRun.hashCode() : 0);
        result = 31 * result + (priority != null ? priority.hashCode() : 0);
        result = 31 * result + (functionName != null ? functionName.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(QueuedJob other) {
        return other.getTimeToRun().compareTo(this.getTimeToRun());
    }
}