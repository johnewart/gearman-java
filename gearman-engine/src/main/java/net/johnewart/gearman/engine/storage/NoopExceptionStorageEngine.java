package net.johnewart.gearman.engine.storage;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoopExceptionStorageEngine implements ExceptionStorageEngine {
    private static Logger LOG = LoggerFactory.getLogger(NoopExceptionStorageEngine.class);

    public NoopExceptionStorageEngine() {
        LOG.info("NoOp exception storage engine initialized, exceptions will not be stored.");
    }

    @Override
    public boolean storeException(String jobHandle, String uniqueId, byte[] jobData, byte[] exceptionData) {
        return true;
    }

    @Override
    public ImmutableList<String> getFailedJobHandles() {
        return null;
    }

    @Override
    public ImmutableList<ExceptionData> getExceptions(int pageNum, int pageSize) {
        return null;
    }

    @Override
    public int getCount() {
        return 0;
    }
}
