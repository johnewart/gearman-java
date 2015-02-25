package net.johnewart.gearman.engine.storage;

import com.google.common.collect.ImmutableList;
import org.joda.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;

public class MemoryExceptionStorageEngine implements ExceptionStorageEngine {

    private static Logger LOG = LoggerFactory.getLogger(PostgresExceptionStorageEngine.class);
    private final Map<String, ExceptionData> exceptionDataMap;
    private final LinkedList<String> jobHandles;
    private final int maxEntries;

    public MemoryExceptionStorageEngine(final int maxEntries) {
        this.exceptionDataMap = new ConcurrentHashMap<>();
        this.jobHandles = new LinkedList<>();
        this.maxEntries = maxEntries;
        LOG.debug("Starting memory exception storage engine with " + maxEntries + " max. entries.");
    }

    @Override
    public boolean storeException(String jobHandle, String uniqueId, byte[] jobData, byte[] exceptionData) {

        synchronized (jobHandles) {
            if (jobHandles.size() == maxEntries) {
                String toRemove = jobHandles.poll();
                exceptionDataMap.remove(toRemove);
            }
        }

        ExceptionData data = new ExceptionData(jobHandle, uniqueId, jobData, exceptionData, new LocalDateTime());
        jobHandles.offer(jobHandle);
        exceptionDataMap.put(jobHandle, data);

        return true;
    }

    @Override
    public ImmutableList<String> getFailedJobHandles() {
        return ImmutableList.copyOf(jobHandles);
    }

    @Override
    public ImmutableList<ExceptionData> getExceptions(int pageNum, int pageSize) {
        int offset = pageSize * (pageNum - 1);
        List<ExceptionData> exceptionDatas = new ArrayList<>(pageSize);
        if(offset < jobHandles.size()) {
            for (int i = 0; i < pageSize; i++) {
                int idx = offset + i;
                if(idx >= getCount()) break;
                String jobHandle = jobHandles.get(idx);
                exceptionDatas.add(exceptionDataMap.get(jobHandle));
            }
        }
        return ImmutableList.copyOf(exceptionDatas);
    }

    @Override
    public int getCount() {
        return jobHandles.size();
    }
}
