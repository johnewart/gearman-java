package org.gearman.server.core;

public final class RunnableJob
{
    public final String uniqueID;
    public final Long whenToRun;

    public RunnableJob(String uniqueID, long whenToRun)
    {
        this.uniqueID = uniqueID;
        this.whenToRun = whenToRun;
    }
}