package org.gearman.server.storage;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.annotation.Metered;
import com.yammer.metrics.annotation.Timed;
import com.yammer.metrics.core.Counter;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.gearman.common.Job;
import org.gearman.common.JobState;
import org.gearman.common.JobStatus;
import org.gearman.common.interfaces.Client;
import org.gearman.common.interfaces.Worker;
import org.gearman.constants.GearmanConstants;
import org.gearman.constants.JobPriority;
import org.gearman.server.exceptions.IllegalJobStateTransitionException;
import org.gearman.server.util.JobHandleFactory;
import org.gearman.server.core.*;
import org.gearman.server.persistence.PersistenceEngine;
import org.gearman.server.util.EqualsLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


/**
 *
 */
public class JobStore {
    public static final Date timeStarted = new Date();

    private static Logger LOG = LoggerFactory.getLogger(JobStore.class);

    // Job Queues: Function Name <--> JobQueue
    private final ConcurrentHashMap<String, JobQueue> jobQueues;

    private final Set<Worker> workers;
    private final ConcurrentHashMap<Worker, Job> workerJobs;

    // Active jobs (job handle <--> Job)
    private final ConcurrentHashMap<String, Job> activeJobs;

    private final EqualsLock lock = new EqualsLock();
    private final PersistenceEngine persistenceEngine;

    private final Counter pendingJobsCounter = Metrics.newCounter(JobStore.class, "pending-jobs");
    private final Counter queuedJobsCounter = Metrics.newCounter(JobStore.class, "queued-jobs");
    private final Counter completedJobsCounter = Metrics.newCounter(JobStore.class, "completed-jobs");
    private final Counter activeJobsCounter = Metrics.newCounter(JobStore.class, "active-jobs");

    public JobStore(PersistenceEngine persistenceEngine)
    {
        this.activeJobs  = new ConcurrentHashMap<>();
        this.jobQueues = new ConcurrentHashMap<>();
        this.persistenceEngine = persistenceEngine;
        this.workers = new ConcurrentHashSet<>();
        this.workerJobs = new ConcurrentHashMap<>();

        // Initialize counters to zero
        this.pendingJobsCounter.clear();
        this.queuedJobsCounter.clear();
        this.completedJobsCounter.clear();
        this.activeJobsCounter.clear();
    }

    public synchronized Job getByJobHandle(String jobHandle)
    {
        return persistenceEngine.findJobByHandle(jobHandle);
    }

    public void registerWorkerAbility(String funcName, Worker worker)
    {
        workers.add(worker);
        getJobQueue(funcName).addWorker(worker);
    }

    public void unregisterWorkerAbility(String funcName, Worker worker)
    {
        getJobQueue(funcName).removeWorker(worker);
    }

    public void unregisterWorker(Worker worker)
    {
        // Remove this worker from the queues
        for(String jobQueueName : worker.getAbilities())
            getJobQueue(jobQueueName).removeWorker(worker);

        // Remove from active worker count
        workers.remove(worker);

        // If this worker has any active jobs, clean up after it
        Job job = workerJobs.get(worker);
        if(job != null)
        {
            JobAction action = disconnectWorker(job, worker);
            switch (action)
            {
                case REENQUEUE:
                    try {
                        reEnqueueJob(job);
                    } catch (IllegalJobStateTransitionException e) {
                        LOG.error("Unable to re-enqueue job: " + e.toString());
                    }
                    break;
                case MARKCOMPLETE:
                    removeJob(job);
                    break;
                case DONOTHING:
                    // Let it go away
                default:
                    break;
            }

            workerJobs.remove(worker);
        }

    }

    public void unregisterClient(Client client)
    {

    }

    public void sleepingWorker(Worker worker)
    {
        for(String jobQueueName : worker.getAbilities())
            getJobQueue(jobQueueName).setWorkerAsleep(worker);
    }

    /**
     * Fetch a job from the job queue based on unique ID
     * @param uniqueID Unique identifier of the job
     * @return Job matching that unique ID in this named queue
     */
    private Job fetchJob(String functionName, String uniqueID)
    {
        return persistenceEngine.findJob(functionName, uniqueID);
    }

    private Job fetchJob(QueuedJob queuedJob)
    {
        return fetchJob(queuedJob.getFunctionName(), queuedJob.getUniqueID());
    }

    @Timed
    @Metered
    public Job nextJobForWorker(Worker worker)
    {
        for(String functionName : worker.getAbilities())
        {
            final JobQueue jobQueue = getJobQueue(functionName);
            final QueuedJob queuedJob = jobQueue.nextJob();
            jobQueue.setWorkerAwake(worker);

            if (queuedJob != null)
            {
                Job job = fetchJob(queuedJob);
                job.setState(JobState.WORKING);

                activeJobs.put(job.getJobHandle(), job);
                workerJobs.put(worker, job);
                pendingJobsCounter.dec();
                activeJobsCounter.inc();

                return job;
            }
        }

        // Nothing found, return null
        return null;
    }

    public synchronized void removeJob(Job job)
    {
        // Remove it from the job queue
        getJobQueue(job.getFunctionName()).remove(new QueuedJob(job));

        // If it's a background job, remove it
        if(job.isBackground() && persistenceEngine != null)
        {
            try {
                persistenceEngine.delete(job);
            } catch (Exception e) {
                // TODO: be more specific
                LOG.debug("Can't remove job from persistence engine: " + e.toString());
                e.printStackTrace();
            }
        }

    }

    public String generateUniqueID(String functionName)
    {
        String uniqueID;
        JobQueue jobQueue = getJobQueue(functionName);

        do {
            uniqueID = new String(UUID.randomUUID().toString().getBytes(GearmanConstants.CHARSET));
        } while(jobQueue.uniqueIdInUse(uniqueID));

        return uniqueID;
    }

    public Job createAndStoreJob(String functionName,
                                 String uniqueID,
                                 byte[] data,
                                 JobPriority priority,
                                 boolean isBackground,
                                 long timeToRun)
    {
        Job job = new Job(functionName, uniqueID, data, priority, isBackground, timeToRun);
        if(storeJob(job))
        {
            return job;
        } else {
            return null;
        }
    }

    public boolean storeJob(Job job)
    {
        queuedJobsCounter.inc();

        final String functionName = job.getFunctionName();
        final String uniqueID = job.getUniqueID();
        final Integer key = uniqueID.hashCode();
        final JobQueue jobQueue = getJobQueue(functionName);

        // Make sure only one thread attempts to add a job with this unique id
        this.lock.lock(key);
        try {
            if(job.getJobHandle() == null || job.getJobHandle().isEmpty())
            {
                job.setJobHandle(JobHandleFactory.getNextJobHandle().toString());
            }
            // Client is submitting a job whose unique ID is in use
            // i.e re-submitting an existing job, ignore and
            // return the existing job.
            if(jobQueue.uniqueIdInUse(uniqueID)) {
                removeJob(job);
            }

            if(!jobQueue.enqueue(new QueuedJob(job)))
            {
                LOG.error("Unable to enqueue job");
                return false;
            } else {
                persistenceEngine.write(job);

                // Notify any workers if this job is ready to run so it
                // gets picked up quickly
                if(job.isReady())
                    jobQueue.notifyWorkers();

                pendingJobsCounter.inc();
                return true;
            }
        } finally {
            // Always unlock lock
            this.lock.unlock(key);
        }
    }

    public final void reEnqueueJob(Job job) throws IllegalJobStateTransitionException
    {
        JobState previousState = job.getState();
        job.setState(JobState.QUEUED);
        final JobQueue jobQueue = getJobQueue(job.getFunctionName());
        switch(previousState) {
            case QUEUED:
                // Do nothing
                break;
            case WORKING:
                // Requeue
                LOG.debug("Re-enqueing job " + job.toString());
                storeJob(job);
                break;
            case COMPLETE:
                throw new IllegalJobStateTransitionException("Jobs should not transition from complete to queued.");
                // should never go from COMPLETE to QUEUED
        }


    }

    @Timed
    @Metered
    public synchronized void workComplete(Job job, Worker worker)
    {
        if(job != null)
        {
            activeJobs.remove(job.getJobHandle());
            job.complete();
            removeJob(job);
            completedJobsCounter.inc();
        }
    }

    public JobStatus checkJobStatus(String jobHandle)
    {
        if(activeJobs.containsKey(jobHandle))
        {
            Job job = activeJobs.get(jobHandle);
            return job.getStatus();
        } else {
            // Not found, so send an "I don't know" answer
            return new JobStatus(0, 0, JobState.UNKNOWN, jobHandle);
        }
    }

    public void updateJobStatus(String jobHandle, int completeNumerator, int completeDenominator)
    {
        if(activeJobs.containsKey(jobHandle))
        {
            Job job = activeJobs.get(jobHandle);
            job.setStatus(completeNumerator, completeDenominator);
        }
    }

    public void loadAllJobs()
    {
        if(persistenceEngine != null)
        {
            Collection<QueuedJob> jobs = null;

            try {
                jobs = persistenceEngine.readAll();
            } catch (Exception ex) {
                // TODO LOG
                LOG.debug("Error loading persistent data: " + ex.toString());
            }

            if(jobs==null) return;
            for(QueuedJob job : jobs) {
                String functionName = job.getFunctionName();
                if(functionName == null) {
                    // TODO log
                    LOG.debug("Error queueing job: functionName is null");
                } else {
                    JobQueue jobQueue = getJobQueue(functionName);
                    try {
                        jobQueue.enqueue(job);
                        pendingJobsCounter.inc();
                    } catch (Exception e) {
                        LOG.error(e.toString());
                    }
                }
            }

            LOG.info("Loaded " + jobs.size() + " jobs from persistent storage.");
        }
    }


    public final JobQueue getJobQueue(String name)
    {
        Integer key = name.hashCode();
        try {
            lock.lock(key);

            JobQueue jobQueue = jobQueues.get(name);

            if(jobQueue==null)
            {
                jobQueue = new JobQueue(name);
                this.jobQueues.put(name, jobQueue);
            }

            return jobQueue;
        } finally {
            lock.unlock(key);
        }
    }

    public ConcurrentHashMap<String, JobQueue> getJobQueues()
    {
        return jobQueues;
    }

    public PersistenceEngine getPersistenceEngine()
    {
        return persistenceEngine;
    }

    public Counter getPendingJobsCounter()
    {
        return pendingJobsCounter;
    }

    public Counter getCompletedJobsCounter()
    {
        return completedJobsCounter;
    }

    public Counter getQueuedJobsCounter()
    {
        return queuedJobsCounter;
    }

    public Counter getActiveJobsCounter()
    {
        return activeJobsCounter;
    }

    public Integer getWorkerCount()
    {
        return workers.size();
    }

    public Job getCurrentJobForWorker(Worker worker) {
        return workerJobs.get(worker);
    }

    public final JobAction disconnectClient(final Job job, final Client client) {
        JobAction result = JobAction.DONOTHING;
        job.getClients().remove(client);

        switch (job.getState()) {
            // If the job was in the QUEUED state, all attached clients have
            // disconnected, and it is not a background job, drop the job
            case QUEUED:
                if(job.getClients().isEmpty() && !job.isBackground())
                    result = JobAction.MARKCOMPLETE;
                break;

            case WORKING:
                if(job.getClients().isEmpty())
                {
                    // The last client disconnected, so be done.
                    result = JobAction.MARKCOMPLETE;
                } else {
                    // (!this.clients.isEmpty() || this.background)==true
                    result = JobAction.REENQUEUE;
                }

                break;

            // Do nothing
            case COMPLETE:
            default:
                result = JobAction.DONOTHING;
        }

        return result;
    }

    public final JobAction disconnectWorker(final Job job, final Worker worker) {

        JobAction result = JobAction.DONOTHING;

        switch (job.getState()) {
            case QUEUED:
                // This should never happen.
                LOG.error("Job in a QUEUED state had a worker disconnect from it. This should not happen.");
                break;

            case WORKING:
                if(job.getClients().isEmpty() && !job.isBackground()) {
                    // Nobody to send it to and it's not a background job,
                    // not much we can do here..
                    result = JobAction.MARKCOMPLETE;
                } else {
                    // (!this.clients.isEmpty() || this.background)==true
                    result = JobAction.REENQUEUE;
                }
                break;

            // Do nothing if it's complete
            case COMPLETE:
            default:
                result = JobAction.DONOTHING;
        }

        return result;
    }

}
