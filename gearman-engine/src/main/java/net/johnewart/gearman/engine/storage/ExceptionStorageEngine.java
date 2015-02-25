package net.johnewart.gearman.engine.storage;

import com.google.common.collect.ImmutableList;

public interface ExceptionStorageEngine {
    public boolean storeException(String jobHandle, String uniqueId, byte[] jobData, byte[] exceptionData);
    public ImmutableList<String> getFailedJobHandles();
    public ImmutableList<ExceptionData> getExceptions(int pageNum, int pageSize);
    public int getCount();
}
