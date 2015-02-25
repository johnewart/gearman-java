package net.johnewart.gearman.embedded;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.interfaces.EngineClient;
import net.johnewart.gearman.common.interfaces.JobHandleFactory;
import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.engine.core.UniqueIdFactory;
import net.johnewart.gearman.engine.queue.factories.JobQueueFactory;
import net.johnewart.gearman.engine.queue.factories.MemoryJobQueueFactory;
import net.johnewart.gearman.engine.storage.ExceptionStorageEngine;
import net.johnewart.gearman.engine.storage.NoopExceptionStorageEngine;
import net.johnewart.gearman.engine.util.LocalJobHandleFactory;
import net.johnewart.gearman.engine.util.LocalUniqueIdFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedGearmanServer {
    private final Logger LOG = LoggerFactory.getLogger(EmbeddedGearmanServer.class);

    final JobManager jobManager;
    final JobHandleFactory jobHandleFactory;
    final UniqueIdFactory uniqueIdFactory;

    public EmbeddedGearmanServer() {
        JobQueueFactory jobQueueFactory = new MemoryJobQueueFactory();
        jobHandleFactory = new LocalJobHandleFactory("embedded");
        uniqueIdFactory = new LocalUniqueIdFactory();
        ExceptionStorageEngine exceptionStore = new NoopExceptionStorageEngine();
        jobManager = new JobManager(jobQueueFactory, jobHandleFactory, uniqueIdFactory, exceptionStore);
    }

    public Job submitJob(final Job job, final EngineClient client) {
        LOG.debug("Submitting job for client...");
        return jobManager.storeJobForClient(job, client);
    }

    public Job getNextJobForWorker(final EmbeddedGearmanWorker worker) {
        return jobManager.nextJobForWorker(worker);
    }

    public void markWorkerAsleep(final EmbeddedGearmanWorker worker) {
        jobManager.markWorkerAsAsleep(worker);
    }

    public void registerWorkerAbility(final EmbeddedGearmanWorker worker,
                                      final String callback) {
        jobManager.registerWorkerAbility(callback, worker);
    }

    public void completeWork(final Job job, final byte[] results) {
        jobManager.handleWorkCompletion(job, results);
    }
}
