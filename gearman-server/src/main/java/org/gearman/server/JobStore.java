package org.gearman.server;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.annotation.Metered;
import com.yammer.metrics.annotation.Timed;
import com.yammer.metrics.core.Counter;
import org.gearman.common.packets.Packet;
import org.gearman.common.packets.request.SubmitJob;
import org.gearman.common.packets.response.NoJob;
import org.gearman.constants.GearmanConstants;
import org.gearman.constants.JobPriority;
import org.gearman.server.persistence.PersistenceEngine;
import org.gearman.server.util.EqualsLock;
import org.gearman.util.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// UG...LEEE.

public class JobStore {
    /**
     * Global Job Map
     * key = job handle
     * value = job
     */

    private static Logger LOG = LoggerFactory.getLogger(JobStore.class);

    private final ConcurrentHashMap<String, JobQueue> jobQueues = new ConcurrentHashMap<>();
    //
    private final ConcurrentHashMap<String, Job > allJobs = new ConcurrentHashMap<>();
    // Jobs associated with a given client
    private final ConcurrentHashMap<Client, Set<Job>> clientJobs = new ConcurrentHashMap<>();
    // Those who are able to do work
    private final ConcurrentHashMap<Client, Set<String>> workers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Client> activeJobs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> pendingCounters = new ConcurrentHashMap<>();

    private final EqualsLock lock = new EqualsLock();
    private final PersistenceEngine persistenceStore;

    private final Counter pendingJobs = Metrics.newCounter(JobStore.class, "pending-jobs");

    public JobStore()
    {

        persistenceStore = null;
    }

    public JobStore(PersistenceEngine persistenceStore)
    {
        this.persistenceStore = persistenceStore;
    }

    public synchronized Job getByJobHandle(String jobHandle){
        return allJobs.get(jobHandle);
    }

    public void registerWorker(String funcName, Client worker)
    {
        if(!this.workers.containsKey(worker))
        {
            this.workers.put(worker, new HashSet<String>());
        }
        this.workers.get(worker).add(funcName);

        // Mark this as someone to wakeup when the function has work
        getJobQueue(funcName).addWorker(worker);
    }

    public void unregisterWorker(String funcName, Client worker)
    {
        if(this.workers.containsKey(worker))
        {
            this.workers.get(worker).remove(funcName);
        }

        // Remove this as something to wakeup when we have work
        getJobQueue(funcName).removeWorker(worker);
    }

    @Timed
    @Metered
    public void nextJobForWorker(Client worker, boolean uniqueID)
    {
        boolean foundJob = false;
        for(String functionName : workers.get(worker))
        {
            LOG.debug("Checking in queue " + functionName);
            final JobQueue jobQueue = getJobQueue(functionName);
            final Job job = jobQueue.nextJob();

            if (job != null)
            {
                activeJobs.put(job.getUniqueID(), worker);
                job.setState(Job.JobState.WORKING);
                job.addClient(worker);

                //worker.addDisconnectListener(this);
                Packet packet;

                if(uniqueID)
                {
                    packet = job.createJobAssignUniqPacket();
                } else {
                    packet = job.createJobAssignPacket();
                }

                foundJob = true;

                try {
                    worker.sendPacket(packet);
                } catch (IOException ioe) {

                        try {
                            enqueueJob(job);
                        } catch (Exception e) {
                            LOG.error("Error re-enqueing after failed transmission: " + e.toString());
                        }
                }
            }
        }

        if(!foundJob) {
            try {
                worker.sendPacket(new NoJob());
            } catch (IOException ioe) {
                LOG.error("Problem sending packet", ioe);
            }
        }
    }

    public synchronized void removeJob(Job job)
    {
        allJobs.remove(job.getJobHandle());
        pendingJobs.dec();
        getJobQueue(job.getFunctionName()).remove(job);

        if(job.isBackground() && persistenceStore != null)
        {
            try {
                persistenceStore.delete(job);
            } catch (Exception e) {
                // TODO: be more specific
                LOG.debug("Can't remove job from persistence engine: " + e.toString());
                e.printStackTrace();
            }
        }
    }

    @Timed
    @Metered
    public final void createJob(SubmitJob packet, Client creator) {
        //creator.addDisconnectListener(this);
        String funcName = packet.getFunctionName();
        String uniqueID = packet.getUniqueId();
        byte[] data = packet.getData();
        JobPriority priority = packet.getPriority();
        boolean isBackground = packet.isBackground();

        pendingJobs.inc();

        JobQueue jobQueue = getJobQueue(funcName);

        if(uniqueID.isEmpty()) {
            do {
                uniqueID = new String(UUID.randomUUID().toString().getBytes(GearmanConstants.CHARSET));
            } while(jobQueue.uniqueIdInUse(new ByteArray(uniqueID)));
        }

        final Integer key = uniqueID.hashCode();
        this.lock.lock(key);
        try {

            // Make sure only one thread attempts to add a job with this uID at once
            if(jobQueue.uniqueIdInUse(new ByteArray(uniqueID))) {

                final Job job = jobQueue.getJobByUniqueId(new ByteArray(uniqueID));
                if(job!=null) {
                    synchronized(job) {
                        // If the job is not background, add creator to listener set and send JOB_CREATED packet
                        if(!isBackground) job.addClient(creator);
                        try{
                            creator.sendPacket(job.createJobCreatedPacket());
                        } catch (IOException ioe) {
                            LOG.error("Problem sending job created packet: ", ioe);
                        }
                        return;
                    }
                }
            }

            final Job job = new Job(funcName, uniqueID, data, priority, isBackground, creator);

            // New job, add it to the job list
            if(!jobQueue.add(job))
            {
                // Oops, full!
                //creator.sendPacket(StaticPackets.ERROR_QUEUE_FULL,null);

            } else {

                try {
                    // If it's backgrounded and there's a persistence engine being used, store it
                    if(isBackground && persistenceStore!=null) {
                        persistenceStore.write(job);
                    }
                } catch(Exception e) {
                    // TODO log exception
                    LOG.debug("Exception: " + e.toString());
                }

                /*
                    * The JOB_CREATED packet must sent before the job is added to the queue.
                    * Queuing the job before sending the packet may result in another thread
                    * grabbing, completing and sending a WORK_COMPLETE packet before the
                    * JOB_CREATED is sent
                    */

                try {
                    creator.sendPacket(job.createJobCreatedPacket());
                } catch (IOException ioe) {
                    LOG.error("Unable to send JOB_CREATED packet: ", ioe);
                }

                /*
                    * The job must be queued before sending the NOOP packets. Sending the noops
                    * first may result in a worker failing to grab the job
                    */

                if(jobQueue.enqueue(job))
                {
                    // Assuming
                    jobQueue.notifyWorkers();
                    allJobs.put(job.getJobHandle(), job);
                }
            }
        } finally {
            // Always unlock lock
            this.lock.unlock(key);
        }
    }

    public final void enqueueJob(Job job) throws IllegalJobStateTransitionException
    {
        pendingJobs.inc();
        Job.JobState previousState = job.getState();
        job.setState(Job.JobState.QUEUED);
        final JobQueue jobQueue = getJobQueue(job.getFunctionName());
        switch(previousState) {
            case QUEUED:
                // Do nothing
                break;
            case WORKING:
                // Requeue
                final boolean value = jobQueue.enqueue(job);
                assert value;
                break;
            case COMPLETE:
                throw new IllegalJobStateTransitionException("Jobs should not transition from complete to queued.");
                // should never go from COMPLETE to QUEUED
        }
    }


    @Timed
    @Metered
    public synchronized void workComplete(Job job, Packet packet)
    {

        Set<Client> clients = job.getClients();

        for(Client client : clients) {
            try {
                client.sendPacket(packet);
            } catch (IOException ioe) {
                LOG.error("Problem notifying client that the work is complete.", ioe);
            }
        }

        job.complete();
        removeJob(job);
    }




    public void loadAllJobs()
    {
        Collection<Job> jobs = null;
        try {
            jobs = persistenceStore.readAll();
        } catch (Exception ex) {
            // TODO LOG
            LOG.debug("Error loading persistent data: " + ex.toString());
        }

        if(jobs==null) return;
        for(Job job : jobs) {
            String functionName = job.getFunctionName();
            if(functionName == null) {
                // TODO log
                LOG.debug("Error queueing job: functionName is null");
            } else {
                LOG.debug("Loading job for " + functionName);
                JobQueue jobQueue = getJobQueue(functionName);
                try {
                    jobQueue.add(job);
                    jobQueue.enqueue(job);
                    allJobs.put(job.getJobHandle(), job);
                    pendingJobs.inc();
                } catch (Exception e) {
                    LOG.error(e.toString());
                }

            }
        }
    }


    public final JobQueue getJobQueue(String name) {
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

    public final void sendStatus(Client client) {

        for(JobQueue jobQueue : jobQueues.values()) {
            if(jobQueue!=null)
            {
                try {
                    client.sendPacket(jobQueue.getStatus());
                } catch (IOException ioe) {
                    LOG.error("Can't send status: ", ioe);
                }
            }
        }

        //client.sendPacket(StaticPackets.TEXT_DONE, null /*TODO*/);
    }

    public final void onDisconnect(final Client client) {
        if(clientJobs.containsKey(client))
        {
            for(Job job : clientJobs.get(client))
            {
                JobQueue jobQueue = getJobQueue(job.getFunctionName());
                Job.JobAction action = job.disconnectClient(client);
                switch (action)
                {
                    case REENQUEUE:
                        try {
                            enqueueJob(job);
                        } catch (Exception e) {
                            LOG.error("Unable to re-enqueue job: " + e.toString());
                        }
                        break;
                    case MARKCOMPLETE:
                        jobQueue.remove(job);
                        break;
                    case DONOTHING:
                    default:
                        break;
                }

            }
        }
    }


}
