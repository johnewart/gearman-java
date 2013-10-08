package net.johnewart.gearman.exceptions;

public class WorkExceptionException extends WorkException {
    private String message;

    public WorkExceptionException(final String jobHandle, final byte[] exceptionData) {
        this.jobHandle = jobHandle;
        this.message = new String(exceptionData);
    }

    public String getMessage() {
        return message;
    }
}
