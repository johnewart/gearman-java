package net.johnewart.gearman.exceptions;

public class WorkException extends Throwable {
    protected String jobHandle;

    public String getJobHandle() {
        return jobHandle;
    }
}
