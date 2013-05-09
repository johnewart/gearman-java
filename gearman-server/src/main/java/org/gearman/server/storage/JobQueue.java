package org.gearman.server.storage;

import com.google.common.collect.ImmutableList;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.gearman.common.interfaces.Worker;
import org.gearman.server.core.QueuedJob;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

public final class JobQueue {
	private final BlockingDeque<QueuedJob> low		= new LinkedBlockingDeque<>();
	private final BlockingDeque<QueuedJob> mid		= new LinkedBlockingDeque<>();
	private final BlockingDeque<QueuedJob> high		= new LinkedBlockingDeque<>();

    private final Logger LOG = LoggerFactory.getLogger(JobQueue.class);

    private final String name;

    // All workers attached to this queue
    private final Set<Worker> workers = new CopyOnWriteArraySet<>();

    // Sleeping workers (those to send an NOOP to when a job arrives)
    private final Set<Worker> sleepingWorkers = new CopyOnWriteArraySet<>();

    // Set of unique IDs in this queue
    private final ConcurrentHashSet<String> allJobs = new ConcurrentHashSet<>();

    private int maxQueueSize = 0;
    private final AtomicLong jobsInQueue;

    public JobQueue(String name)
    {
        this.jobsInQueue = new AtomicLong(0);
        this.name = name;

        Metrics.newGauge(JobQueue.class, "pending-" + this.metricName(), new Gauge<Long>() {
            @Override
            public Long value() {
                return jobsInQueue.longValue();
            }
        });

    }

    public final boolean enqueue(QueuedJob runnableJob)
    {
        synchronized (this.allJobs) {
            if(this.maxQueueSize>0 && maxQueueSize<=jobsInQueue.longValue()) {
                return false;
            }

            if(allJobs.contains(runnableJob.getUniqueID()))
            {
                return false;
            } else {
                jobsInQueue.incrementAndGet();
                allJobs.add(runnableJob.getUniqueID());


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
        }
    }

    /**
     * Fetch the next job waiting -- this checks high, then normal, then low
     * Caveat: in the normal queue, we skip over any jobs whose timestamp has not
     * come yet (support for epoch jobs)
     *
     * @return Next Job in the queue, null if none
     */
	public final QueuedJob poll() {

        long currentTime = new DateTime().toDate().getTime() / 1000;

        // High has no epoch jobs
        QueuedJob runnableJob = high.poll();

        if (runnableJob == null)
        {
            // Check to see which, if any, need to be run now
            synchronized (mid)
            {
                for(QueuedJob rj : mid)
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
            return runnableJob;
        }

        return null;
    }

 	/**
	 * Returns the total number of jobs in this queue
	 * @return
	 * 		The total number of jobs in all priorities
	 */
	public final int size() {
		return jobsInQueue.intValue();
	}

    public final boolean uniqueIdInUse(String uniqueID)
    {
        return allJobs.contains(uniqueID);
    }

    public final void setWorkerAsleep(final Worker worker)
    {
        this.sleepingWorkers.add(worker);
    }

    public final void setWorkerAwake(final Worker worker)
    {
        this.sleepingWorkers.remove(worker);
    }

    public final void addWorker(final Worker worker) {
        workers.add(worker);
    }

    public final void removeWorker(final Worker worker) {
        workers.remove(worker);
        sleepingWorkers.remove(worker);
    }

	public final boolean isEmpty() {
		return high.isEmpty() && mid.isEmpty() && low.isEmpty();
	}

    public final void setMaxQueue(final int size) {
        synchronized(this.allJobs) { this.maxQueueSize = size; }
    }

    public void notifyWorkers()
    {
        for(Worker worker : sleepingWorkers) {
            try {
                worker.wakeUp();
            } catch (Exception e) {
                LOG.error("Unable to wake up worker...");
            }
        }
    }

    public final String getName() {
        return this.name;
    }

    public final QueuedJob nextJob() {
        final QueuedJob job = this.poll();
        if (job != null)
        {
            allJobs.remove(job.getUniqueID());
        }
        return job;
    }

    public final boolean remove(QueuedJob queuedJob)
    {
        if(queuedJob == null)
            return false;

        if(allJobs.contains(queuedJob.getUniqueID()))
        {
            jobsInQueue.decrementAndGet();
            allJobs.remove(queuedJob.getUniqueID());
            switch (queuedJob.getPriority()) {
                case LOW:
                    low.remove(queuedJob);
                    break;
                case NORMAL:
                    mid.remove(queuedJob);
                    break;
                case HIGH:
                    high.remove(queuedJob);
                    break;
            }

            return true;
        }

        return false;
    }

    public String metricName()
    {
        return this.name.toString().replaceAll(":", ".");
    }

    public Collection<QueuedJob> getAllJobs() {
        Set<QueuedJob> jobs = new HashSet<>();

        synchronized (low)
        {
            jobs.addAll(low);
        }

        synchronized (mid)
        {
            jobs.addAll(mid);
        }

        synchronized (high)
        {
            jobs.addAll(high);
        }

        return jobs;
    }

    public HashMap<String, ImmutableList<QueuedJob>> getCopyOfJobQueues()
    {
        HashMap<String, ImmutableList<QueuedJob>> queues = new HashMap<>();

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
