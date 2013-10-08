package net.johnewart.gearman.exceptions;

public class WorkFailException extends WorkException {
    public WorkFailException(final String jobHandle) {
        this.jobHandle = jobHandle;
    }
}
