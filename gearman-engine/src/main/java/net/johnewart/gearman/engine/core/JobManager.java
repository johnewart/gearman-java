package net.johnewart.gearman.engine.core;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.annotation.Metered;
import com.yammer.metrics.annotation.Timed;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.JobState;
import net.johnewart.gearman.common.JobStatus;
import net.johnewart.gearman.common.interfaces.EngineClient;
import net.johnewart.gearman.common.interfaces.EngineWorker;
import net.johnewart.gearman.common.interfaces.JobHandleFactory;
import net.johnewart.gearman.engine.exceptions.IllegalJobStateTransitionException;
import net.johnewart.gearman.engine.exceptions.JobQueueFactoryException;
import net.johnewart.gearman.engine.queue.JobQueue;
import net.johnewart.gearman.engine.queue.factories.JobQueueFactory;
import net.johnewart.gearman.engine.util.EqualsLock;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


public class JobManager {
    public static final Date timeStarted = new Date();

    private static Logger LOG = LoggerFactory.getLogger(JobManager.class);

    // Job Queues: Function Name <--> JobQueue
    private final ConcurrentHashMap<String, JobQueue> jobQueues;
    private final ConcurrentHashMap<EngineWorker, Job> workerJobs;
    private final ConcurrentHashMap<Job, EngineWorker> jobWorker;
    // Active jobs (job handle <--> Job)
    private final ConcurrentHashMap<String, Job> activeJobHandles;
    private final ConcurrentHashMap<String, Job> activeUniqueIds;
    private final ConcurrentHashMap<String, WorkerPool> workerPools;

    // Jobs clients are waiting on (unique id <--> clients)
    private final ConcurrentHashMap<String, Set<EngineClient>> uniqueIdClients;

    private final Set<EngineWorker> workers;
    private final EqualsLock lock = new EqualsLock();
    private final JobQueueFactory jobQueueFactory;
    private final JobHandleFactory jobHandleFactory;
    private final UniqueIdFactory uniqueIdFactory;

    private final Counter pendingJobsCounter = Metrics.newCounter(JobManager.class, "pending-jobs");
    private final Counter queuedJobsCounter = Metrics.newCounter(JobManager.class, "queued-jobs");
    private final Counter completedJobsCounter = Metrics.newCounter(JobManager.class, "completed-jobs");
    private final Counter activeJobsCounter = Metrics.newCounter(JobManager.class, "active-jobs");
    private final Meter jobMeter = Metrics.newMeter(JobManager.class, "queued-jobs-meter", "enqueued", TimeUnit.SECONDS);


    public JobManager(JobQueueFactory jobQueueFactory, JobHandleFactory jobHandleFactory, UniqueIdFactory uniqueIdFactory) {
        this.activeJobHandles = new ConcurrentHashMap<>();
        this.activeUniqueIds = new ConcurrentHashMap<>();
        this.uniqueIdClients = new ConcurrentHashMap<>();
        this.jobQueues = new ConcurrentHashMap<>();
        this.workers = new ConcurrentHashSet<>();
        this.workerJobs = new ConcurrentHashMap<>();
        this.jobWorker = new ConcurrentHashMap<>();
        this.workerPools = new ConcurrentHashMap<>();

        this.jobQueueFactory = jobQueueFactory;
        this.jobHandleFactory = jobHandleFactory;
        this.uniqueIdFactory = uniqueIdFactory;

        // Initialize counters to zero
        this.pendingJobsCounter.clear();
        this.queuedJobsCounter.clear();
        this.completedJobsCounter.clear();
        this.activeJobsCounter.clear();
    }

    public void registerWorkerAbility(String funcName, EngineWorker worker)
    {
        workers.add(worker);
        getWorkerPool(funcName).addWorker(worker);
    }

    public void unregisterWorkerAbility(String funcName, EngineWorker worker)
    {
        getWorkerPool(funcName).removeWorker(worker);
    }

    public void unregisterWorker(EngineWorker worker)
    {
        // Remove this worker from the queues
        for(String jobQueueName : worker.getAbilities())
            getWorkerPool(jobQueueName).removeWorker(worker);

        // Remove from active worker count
        workers.remove(worker);

        // If this worker has any active jobs, clean up after it
        Job job = getCurrentJobForWorker(worker);
        if(job != null)
        {
            JobAction action = disconnectWorker(job, worker);
            removeJob(job);

            switch (action)
            {
                case REENQUEUE:
                    try {
                        reEnqueueJob(job);
                    } catch (IllegalJobStateTransitionException e) {
                        LOG.error("Unable to re-enqueue job: " + e.toString());
                    }
                    break;
                // Let it go away
                case MARKCOMPLETE:
                case DONOTHING:
                default:
                    break;
            }

            workerJobs.remove(worker);
        }

    }

    public void unregisterClient(EngineClient client)
    {
        removeClientForUniqueId(client.getCurrentJob(), client);
    }

    public void markWorkerAsAsleep(EngineWorker worker)
    {
        boolean workWaiting = false;
        for(String jobQueueName : worker.getAbilities()) {
            getWorkerPool(jobQueueName).markSleeping(worker);

            if(getJobQueue(jobQueueName).size() > 0) {
                workWaiting = true;
            }
        }

        if(workWaiting)
            worker.wakeUp();


    }

    @Timed
    @Metered
    public Job nextJobForWorker(EngineWorker worker)
    {
        for(String functionName : worker.getAbilities())
        {
            final JobQueue jobQueue = getJobQueue(functionName);
            final Job job = jobQueue.poll();

            if (job != null)
            {
                final WorkerPool workerPool = getWorkerPool(job.getFunctionName());
                workerPool.markAwake(worker);

                activeJobHandles.put(job.getJobHandle(), job);
                activeUniqueIds.put(job.getUniqueID(), job);
                workerJobs.put(worker, job);
                jobWorker.put(job, worker);
                pendingJobsCounter.dec();
                activeJobsCounter.inc();
                job.markInProgress();
                return job;
            }
        }

        // Nothing found, or couldn't lock -- return null
        return null;
    }

    public synchronized void removeJob(Job job)
    {
        // Remove it from the job queue
        getJobQueue(job.getFunctionName()).remove(job);

        // Remove it from our local tracking maps
        activeJobHandles.remove(job.getJobHandle());
        activeUniqueIds.remove(job.getUniqueID());
        uniqueIdClients.remove(job.getUniqueID());
        EngineWorker worker = jobWorker.remove(job);

        if(worker != null)
            workerJobs.remove(worker);

    }

    public String generateUniqueID(String functionName)
    {
        String uniqueID;
        JobQueue jobQueue = getJobQueue(functionName);

        do {
            uniqueID = uniqueIdFactory.generateUniqueId();
        } while(jobQueue.uniqueIdInUse(uniqueID));

        return uniqueID;
    }

    public Job storeJobForClient(Job job, EngineClient client)
    {
        if(!job.isBackground()) {
            addClientForUniqueId(job.getUniqueID(), client);
        }
        return storeJob(job);
    }

    public Job storeJob(Job job)
    {
        jobMeter.mark();
        final String functionName = job.getFunctionName();
        final String uniqueID;
        final JobQueue jobQueue = getJobQueue(functionName);

        if(job.getUniqueID().isEmpty()) {
            uniqueID = generateUniqueID(functionName);
        } else {
            uniqueID = job.getUniqueID();
        }

        // Make sure only one thread attempts to add a job with this unique id
        final Integer key = uniqueID.hashCode();
        this.lock.lock(key);
        try {

            if(activeUniqueIds.containsKey(uniqueID))
            {
                // If the job is already being processed, pull from active jobs
                return activeUniqueIds.get(uniqueID);
            } else if (jobQueue.uniqueIdInUse(uniqueID)) {
                // If the job is queued but not active, pull it from storage
                return jobQueue.findJobByUniqueId(uniqueID);
            } else {
                // New job, store it in the queue and storage
                if(job.getJobHandle() == null || job.getJobHandle().isEmpty())
                {
                    job.setJobHandle(new String(jobHandleFactory.getNextJobHandle()));
                }

                // Client is submitting a job whose unique ID is in use
                // i.e re-submitting an existing job, ignore and
                // return the existing job.
                if(!jobQueue.enqueue(job))
                {
                    LOG.error("Unable to enqueue job");
                    return null;
                } else {

                    // Notify any workers if this job is ready to run so it
                    // gets picked up quickly
                    if(job.isReady()) {
                        getWorkerPool(job.getFunctionName())
                                .wakeupWorkers();
                    }

                    queuedJobsCounter.inc();
                    pendingJobsCounter.inc();
                    return job;
                }
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
    public synchronized void handleWorkCompletion(Job job, byte[] data)
    {

        if(job != null)
        {
            if(!job.isBackground())
            {
                notifyClientsOfCompletion(job, data);
            }

            completedJobsCounter.inc();
            job.complete();
            removeJob(job);
        }
    }

    public synchronized void handleWorkData(Job job, byte[] data)
    {
        if(job != null && !job.isBackground())
        {
            for(EngineClient client : getClientsForUniqueId(job.getUniqueID()))
            {
                client.sendWorkData(job.getJobHandle(), data);
            }
        }
    }

    public synchronized void handleWorkException(Job job, byte[] exception)
    {
        if(job != null && !job.isBackground()) {
            for(EngineClient client : getClientsForUniqueId(job.getUniqueID()))
            {
                client.sendWorkException(job.getJobHandle(), exception);
            }

            job.complete();
            removeJob(job);
        }
    }

    public synchronized void handleWorkWarning(Job job, byte[] warning)
    {
        if(job != null && !job.isBackground())
        {
            Set<EngineClient> clients = getClientsForUniqueId(job.getUniqueID());

            for(EngineClient client : clients)
            {
                client.sendWorkWarning(job.getJobHandle(), warning);
            }
        }
    }

    public synchronized void handleWorkFailure(Job job)
    {
        if(job != null && !job.isBackground())
        {
            Set<EngineClient> clients = getClientsForUniqueId(job.getUniqueID());

            for(EngineClient client : clients)
            {
                client.sendWorkFail(job.getJobHandle());
            }

        }
    }

    public JobStatus checkJobStatus(String jobHandle)
    {
        if(activeJobHandles.containsKey(jobHandle))
        {
            Job job = activeJobHandles.get(jobHandle);
            return job.getStatus();
        } else {
            // Not found, so send an "I don't know" answer
            return new JobStatus(0, 0, JobState.UNKNOWN, jobHandle);
        }
    }

    public void updateJobStatus(String jobHandle, int completeNumerator, int completeDenominator)
    {
        if(activeJobHandles.containsKey(jobHandle))
        {
            Job job = activeJobHandles.get(jobHandle);
            job.setStatus(completeNumerator, completeDenominator);
            JobStatus status = checkJobStatus(jobHandle);

            if(status != null)
            {
                Set<EngineClient> clients = uniqueIdClients.get(job.getUniqueID());

                if(clients != null && clients.size() > 0)
                {
                    for(EngineClient client : clients)
                    {
                        client.sendWorkStatus(status);
                    }
                }
            }
        }
    }


    public final WorkerPool getWorkerPool(final String name) {
        Integer key = name.hashCode();
        try {
            lock.lock(key);

            WorkerPool workerPool = workerPools.get(name);

            if (workerPool == null) {
                workerPool = new WorkerPool(name);
                this.workerPools.put(name, workerPool);
            }

            return workerPool;
        } finally {
            lock.unlock(key);
        }
    }

    public final JobQueue getJobQueue(String name)
    {
        Integer key = name.hashCode();
        try {
            lock.lock(key);

            JobQueue jobQueue = jobQueues.get(name);

            if(jobQueue == null)
            {
                try {
                    jobQueue = jobQueueFactory.build(name);
                    this.jobQueues.put(name, jobQueue);
                } catch (JobQueueFactoryException jfe) {
                    return null;
                }
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

    public Job getCurrentJobForWorker(EngineWorker worker) {
        return workerJobs.get(worker);
    }

    private void removeClientForUniqueId(Job job, EngineClient client)
    {
        String uniqueID = job.getUniqueID();
        Set<EngineClient> clients = getClientsForUniqueId(uniqueID);
        clients.remove(client);
    }

    private void addClientForUniqueId(String uniqueID, EngineClient client)
    {
        Set<EngineClient> clients = getClientsForUniqueId(uniqueID);
        clients.add(client);
    }

    private Set<EngineClient> getClientsForUniqueId(String uniqueID)
    {
        if(uniqueIdClients.containsKey(uniqueID))
            return uniqueIdClients.get(uniqueID);
        else {
            Set<EngineClient> clients = new ConcurrentHashSet<>();
            uniqueIdClients.put(uniqueID, clients);
            return clients;
        }
    }

    public final JobAction disconnectClient(final Job job, final EngineClient client) {
        JobAction result = JobAction.DONOTHING;

        if(!job.isBackground()) {
            Set<EngineClient> clients = getClientsForUniqueId(job.getUniqueID());

            switch (job.getState()) {
                // If the job was in the QUEUED state, all attached clients have
                // disconnected, and it is not a background job, drop the job
                case QUEUED:
                    if(clients.isEmpty() && !job.isBackground())
                        result = JobAction.MARKCOMPLETE;
                    break;

                case WORKING:
                    if(clients.isEmpty())
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
        }

        return result;
    }

    public final JobAction disconnectWorker(final Job job, final EngineWorker worker) {

        JobAction result = JobAction.DONOTHING;

        if(job.isBackground()) {
            result = JobAction.REENQUEUE;
        } else {
            Set<EngineClient> clients = getClientsForUniqueId(job.getUniqueID());

            switch (job.getState()) {
                case QUEUED:
                    // This should never happen.
                    LOG.error("Job in a QUEUED state had a worker disconnect from it. This should not happen.");
                    break;

                case WORKING:
                    if(clients.isEmpty()) {
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
        }

        return result;
    }

    protected Job getActiveJobByUniqueId(String uniqueId) {
        return activeUniqueIds.get(uniqueId);
    }

    protected void notifyClientsOfCompletion(Job job, byte[] data) {
        Set<EngineClient> clients = getClientsForUniqueId(job.getUniqueID());

        if(!clients.isEmpty())
        {
            for(EngineClient client : clients)
            {
                client.sendWorkResults(job.getJobHandle(), data);
            }
        }
    }

    public Job getJobByJobHandle(final String jobHandle) {
        return activeJobHandles.get(jobHandle);
    }
}
