package net.johnewart.gearman.engine.storage;

import org.joda.time.LocalDateTime;

public class ExceptionData {

    public final String jobHandle;
    public final String uniqueId;
    public final byte[] jobData;
    public final byte[] exceptionData;
    public final LocalDateTime when;

    public ExceptionData(String jobHandle, String uniqueId, byte[] jobData, byte[] exceptionData, LocalDateTime when) {
        this.jobHandle = jobHandle;
        this.uniqueId = uniqueId;
        this.jobData = jobData;
        this.exceptionData = exceptionData;
        this.when = when;
    }

}
