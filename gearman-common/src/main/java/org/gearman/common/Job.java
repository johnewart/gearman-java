package org.gearman.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.gearman.constants.JobPriority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;

public class Job {
    private static Logger LOG = LoggerFactory.getLogger(Job.class);

    private JobPriority priority;
    private boolean background;
    private String functionName;
    private String uniqueID;
    private String jobHandle;
    private byte[] data;
    private int numerator;
    private int denominator;
    private long timeToRun;
    private JobState state = JobState.QUEUED;

    public Job()
    {
        background = false;
        functionName = null;
        uniqueID     = null;
        jobHandle    = null;
        data         = null;
        priority     = JobPriority.NORMAL;
        timeToRun    = -1;
    }

    public Job(final String functionName,
               final String uniqueID,
               final byte[] data,
               final JobPriority priority,
               boolean isBackground)

    {
        this(functionName, uniqueID, data, priority, isBackground, -1);
    }

    public Job(final String functionName,
               final String uniqueID,
               final byte[] data,
               final JobPriority priority,
               boolean isBackground,
               long timeToRun)
    {
        this.functionName = functionName;
        this.uniqueID = uniqueID;
        this.data = data.clone();
        this.priority = priority;
        this.timeToRun = timeToRun;
        this.background = isBackground;
    }

    public Job(final String functionName,
               final String uniqueID,
               final byte[] data,
               final byte[] jobHandle,
               final JobPriority priority,
               boolean isBackground,
               long timeToRun)
    {
        this.functionName = functionName;
        this.uniqueID = uniqueID;
        this.data = data.clone();
        this.priority = priority;
        this.timeToRun = timeToRun;
        this.background = isBackground;
        this.jobHandle = new String(jobHandle);
    }


    public String getFunctionName() {
        return functionName;
    }


    public byte[] getData() {
        return this.data;
    }

    public String getJobHandle() {
        return this.jobHandle;
    }

    public void setJobHandle(String jobHandle) {
        this.jobHandle = jobHandle;
    }

    public JobPriority getPriority() {
        return this.priority;
    }

    public JobState getState() {
        return this.state;
    }

    public void setState(JobState state)
    {
        this.state = state;
    }

    public long getTimeToRun() {
        return timeToRun;
    }

    public String getUniqueID() {
        return this.uniqueID;
    }

    public void setUniqueID(String uniqueID) {
        this.uniqueID = uniqueID;
    }

    public boolean isBackground() {
        return this.background;
    }

    public void setStatus(int numerator, int denominator) {
        this.numerator = numerator;
        this.denominator = denominator;
    }

    public int getNumerator() {
        return numerator;
    }

    public int getDenominator() {
        return denominator;
    }

    public final void complete() {
        this.state = JobState.COMPLETE;
    }

    @JsonIgnore
    public String toString()
    {
        return this.getJobHandle();
    }

    @JsonIgnore
    public JobStatus getStatus()
    {
        return new JobStatus(numerator, denominator, state, jobHandle);
    }

    @JsonIgnore
    public boolean isReady()
    {
        return this.timeToRun < (new Date().getTime() / 1000);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Job job = (Job) o;

        if (background != job.background) return false;
        if (!Arrays.equals(data, job.data)) return false;
        if (functionName != null ? !functionName.equals(job.functionName) : job.functionName != null) return false;
        if (priority != job.priority) return false;
        if (uniqueID != null ? !uniqueID.equals(job.uniqueID) : job.uniqueID != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = functionName != null ? functionName.hashCode() : 0;
        result = 31 * result + (uniqueID != null ? uniqueID.hashCode() : 0);
        return result;
    }
}