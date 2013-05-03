package org.gearman.server;

import com.google.common.collect.ImmutableList;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.gearman.common.packets.response.NoOp;
import org.gearman.server.core.RunnableJob;
import org.gearman.server.persistence.PersistenceEngine;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

public final class JobQueue {
	private final BlockingDeque<RunnableJob> low		= new LinkedBlockingDeque<RunnableJob>();
	private final BlockingDeque<RunnableJob> mid		= new LinkedBlockingDeque<RunnableJob>();
	private final BlockingDeque<RunnableJob> high		= new LinkedBlockingDeque<RunnableJob>();

    private final Logger LOG = LoggerFactory.getLogger(JobQueue.class);

    private final String name;

    // All workers attached to this queue
    private final Set<Channel> workers = new CopyOnWriteArraySet<>();

    // Sleeping workers (those to send an NOOP to when a job arrives)
    private final Set<Channel> sleepingWorkers = new CopyOnWriteArraySet<>();

    // Set of unique IDs in this queue
    private final ConcurrentHashSet<String> allJobs = new ConcurrentHashSet<>();

    private int maxQueueSize = 0;
    private final AtomicLong jobsInQueue;
    private final PersistenceEngine persistenceEngine;

    public JobQueue(String name, PersistenceEngine persistenceEngine)
    {
        this.jobsInQueue = new AtomicLong(0);
        this.name = name;
        this.persistenceEngine = persistenceEngine;

        Metrics.newGauge(JobQueue.class, "pending-" + this.metricName(), new Gauge<Long>() {
            @Override
            public Long value() {
                return jobsInQueue.longValue();
            }
        });

    }

    public final boolean enqueue(RunnableJob runnableJob)
    {
        synchronized (this.allJobs) {
            if(this.maxQueueSize>0 && maxQueueSize<=jobsInQueue.longValue()) {
                return false;
            }

            jobsInQueue.incrementAndGet();

            allJobs.add(runnableJob.uniqueID);
       }

       switch (runnableJob.getPriority()) {
            case LOW:
                synchronized(low)
                {
                    return low.add(runnableJob);
                }
            case NORMAL:
                synchronized(mid) {
                    return mid.add(runnableJob);
                }
            case HIGH:
                synchronized(high) {
                    return high.add(runnableJob);
                }
            default:
                return false;
       }
    }

    // Enqueue a job in the correct job queue
    public final boolean enqueue(Job job)
    {
        if(job != null)
        {
            persistenceEngine.write(job);
            return enqueue(job.getRunnableJob());
        } else {
            return false;
        }
	}

    /**
     * Fetch the next job waiting -- this checks high, then normal, then low
     * Caveat: in the normal queue, we skip over any jobs whose timestamp has not
     * come yet (support for epoch jobs)
     *
     * @return Next Job in the queue, null if none
     */
	public final Job poll() {
        long currentTime = new Date().getTime() / 1000;

        // High has no epoch jobs
        RunnableJob runnableJob = high.poll();

        if (runnableJob == null)
        {
            // Check to see which, if any, need to be run now
            synchronized (mid)
            {
                for(RunnableJob rj : mid)
                {
                    if(rj.timeToRun < currentTime)
                    {
                        if(mid.remove(rj))
                        {
                            runnableJob = rj;
                            break;
                        }
                    }
                }
            }
        }

        if(runnableJob == null)
            runnableJob = low.poll();

        if (runnableJob != null)
        {
            jobsInQueue.decrementAndGet();
            return fetchJob(runnableJob.getUniqueID());
        }

        return null;
    }

    /**
     * Fetch a job from the job queue based on unique ID
     * @param uniqueID Unique identifier of the job
     * @return Job matching that unique ID in this named queue
     */

    private Job fetchJob(String uniqueID)
    {
        return persistenceEngine.findJob(this.name, uniqueID);
    }

	/**
	 * Returns the total number of jobs in this queue
	 * @return
	 * 		The total number of jobs in all priorities
	 */
	public final int size() {
		return jobsInQueue.intValue();
	}

	public final boolean remove(Job job) {
		if(job == null)
			return false;

        if(allJobs.contains(job.getUniqueID()))
        {
            jobsInQueue.decrementAndGet();
            allJobs.remove(job.getUniqueID());

            switch (job.getPriority()) {
                case LOW:
                    return low.remove(job);
                case NORMAL:
                    return mid.remove(job);
                case HIGH:
                    return high.remove(job);
            }
        }

		return false;
	}

    public final boolean uniqueIdInUse(String uniqueID)
    {
        return allJobs.contains(uniqueID);
    }

    public final void setWorkerAsleep(final Channel worker)
    {
        this.sleepingWorkers.add(worker);
    }

    public final void setWorkerAwake(final Channel worker)
    {
        this.sleepingWorkers.remove(worker);
    }

    public final void addWorker(final Channel worker) {
        workers.add(worker);
    }
    public final void removeWorker(final Channel worker) {
        workers.remove(worker);
        sleepingWorkers.remove(worker);
    }

	public final boolean isEmpty() {
		return high.isEmpty() && mid.isEmpty() && low.isEmpty();
	}

    public final Job getJobByUniqueId(String uniqueId)
    {
        if(allJobs.contains(uniqueId))
        {
            return fetchJob(uniqueId);
        } else {
            return null;
        }
    }

    public final void setMaxQueue(final int size) {
        synchronized(this.allJobs) { this.maxQueueSize = size; }
    }

    public void notifyWorkers()
    {
        for(Channel worker : sleepingWorkers) {
            try {
                worker.write(new NoOp());
            } catch (Exception e) {
                LOG.error("Unable to wake up worker...");
            }
        }
    }

    public final String getName() {
        return this.name;
    }

    public final Job nextJob() {
        final Job job = this.poll();
        if (job != null)
        {
            allJobs.remove(job.getUniqueID());
        }
        return job;
    }

    public String metricName()
    {
        return this.name.toString().replaceAll(":", ".");
    }

    public Collection<RunnableJob> getAllJobs() {
        return persistenceEngine.getAllForFunction(this.name);
    }

    public HashMap<String, ImmutableList<RunnableJob>> getCopyOfJobQueues()
    {
        HashMap<String, ImmutableList<RunnableJob>> queues = new HashMap<>();

        synchronized(high) {
            queues.put("high", ImmutableList.copyOf(high));
        }

        synchronized(mid) {
            queues.put("mid",  ImmutableList.copyOf(mid));
        }

        synchronized(low) {
            queues.put("low",  ImmutableList.copyOf(low));
        }

        return queues;
    }

    public Integer getNumberOfConnectedWorkers()
    {
        return workers.size();
    }


}
