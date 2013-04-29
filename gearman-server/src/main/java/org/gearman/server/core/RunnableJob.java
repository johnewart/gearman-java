package org.gearman.server.core;

import org.gearman.constants.JobPriority;

public final class RunnableJob
{
    public final String uniqueID;
    public final Long timeToRun;
    public final JobPriority priority;
    public final String functionName;

    public RunnableJob(String uniqueID, long timeToRun, JobPriority priority, String functionName)
    {
        this.uniqueID = uniqueID;
        this.timeToRun = timeToRun;
        this.priority = priority;
        this.functionName = functionName;
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
}