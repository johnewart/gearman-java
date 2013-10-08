package net.johnewart.gearman.embedded;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.interfaces.EngineClient;
import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.engine.queue.factories.JobQueueFactory;
import net.johnewart.gearman.engine.queue.factories.MemoryJobQueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedGearmanServer {
    private final Logger LOG = LoggerFactory.getLogger(EmbeddedGearmanServer.class);

    final JobManager jobManager;

    public EmbeddedGearmanServer() {
        JobQueueFactory jobQueueFactory = new MemoryJobQueueFactory();
        jobManager = new JobManager(jobQueueFactory);
    }

    public Job submitJob(final Job job, final EngineClient client) {
        LOG.debug("Submitting job for client...");
        return jobManager.storeJobForClient(job, client);
    }

    public Job getNextJobForWorker(final EmbeddedGearmanWorker worker) {
        return jobManager.nextJobForWorker(worker);
    }

    public void markWorkerAsleep(final EmbeddedGearmanWorker worker) {
        jobManager.sleepingWorker(worker);
    }

    public void registerWorkerAbility(final EmbeddedGearmanWorker worker,
                                      final String callback) {
        jobManager.registerWorkerAbility(callback, worker);
    }

    public void completeWork(final Job job, final byte[] results) {
        jobManager.workComplete(job, results);
    }
}
