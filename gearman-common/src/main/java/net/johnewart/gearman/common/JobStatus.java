package net.johnewart.gearman.common;

public class JobStatus {
    private final int numerator;
    private final int denominator;
    private final JobState state;
    private final boolean running;
    private final boolean statusKnown;
    private final String jobHandle;

    public JobStatus(int numerator,
                     int denominator,
                     JobState state,
                     String jobHandle)
    {
        this.state = state;
        this.numerator = numerator;
        this.denominator = denominator;
        this.running = this.state == JobState.WORKING;
        this.statusKnown = (denominator != 0);
        this.jobHandle = jobHandle;
    }

    public int getNumerator() {
        return numerator;
    }

    public int getDenominator() {
        return denominator;
    }

    public JobState getState() {
        return state;
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isStatusKnown() {
        return statusKnown;
    }

    public String getJobHandle() {
        return jobHandle;
    }
}
