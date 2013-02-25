/*
 * Copyright (c) 2012, Isaiah van der Elst (isaiah.v@comcast.net)
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * - Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *   
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *   
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EJobPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EJobEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package org.gearman.server;

import com.google.common.collect.ImmutableList;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.gearman.common.packets.Packet;
import org.gearman.common.packets.response.NoOp;
import org.gearman.server.core.RunnableJob;
import org.gearman.server.persistence.PersistenceEngine;
import org.gearman.util.ByteArray;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

import static com.yammer.metrics.Metrics.newGauge;


/**
 * A JobQueue queues the different jobs in three different priority levels, low,
 * medium, and high.  Jobs pulled from this queue are pulled from the highest
 * priority first, then medium priority, and low priority last.
 *
 * @author isaiah
 *
 */
public final class JobQueue {
	private final BlockingDeque<RunnableJob> low		= new LinkedBlockingDeque<RunnableJob>();
	private final BlockingDeque<RunnableJob> mid		= new LinkedBlockingDeque<RunnableJob>();
	private final BlockingDeque<RunnableJob> high		= new LinkedBlockingDeque<RunnableJob>();

    private final Logger LOG = LoggerFactory.getLogger(JobQueue.class);

    private final String name;
    /** The list of workers waiting for jobs to be placed in the queue */
    private final Set<Channel> workers = new CopyOnWriteArraySet<>();
    private final Set<Channel> sleepingWorkers = new CopyOnWriteArraySet<>();

    /** The set of jobs created by this function. ByteArray is equal to the uID */
    private final ConcurrentHashSet<String> allJobs = new ConcurrentHashSet<>();

    /** The maximum number of jobs this function can have at any one time */
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


    // Enqueue a job in the correct job queue
    // used when re-enqueuing
    public final boolean enqueue(Job job, boolean persist)
    {
        if(job != null)
        {
            synchronized (this.allJobs) {
                if(this.maxQueueSize>0 && maxQueueSize<=jobsInQueue.longValue()) {
                    return false;
                }

                jobsInQueue.incrementAndGet();
                allJobs.add(job.getUniqueID());
            }

            RunnableJob runnableJob = new RunnableJob(job.getUniqueID(), job.getTimeToRun());

            if(persist)
                persistenceEngine.write(job);

            switch (job.getPriority()) {
                case LOW:
                    return low.add(runnableJob);
                case NORMAL:
                    return mid.add(runnableJob);
                case HIGH:
                    return high.add(runnableJob);
                default:
                    return false;
            }
        } else {
            return false;
        }
	}

	/**
	 * Polls the next available job
	 * @return
	 * 		The next job if one is available. null is returned if no job is available
	 */
	public final Job poll() {
        long currentTime = new Date().getTime() / 1000;

        // High has no epoch jobs
        RunnableJob runnableJob = high.poll();

        if (runnableJob != null)
            return fetchJob(runnableJob);

        // Check to see which, if any, need to be run now
        for(RunnableJob rj : mid)
        {
            if(rj.whenToRun < currentTime)
            {
                if(mid.remove(rj))
                {
                    return fetchJob(rj);
                }
            }
        }

        runnableJob = low.poll();

        if (runnableJob != null)
            return fetchJob(runnableJob);

        return null;
    }


    private Job fetchJob(RunnableJob runnableJob)
    {
        return persistenceEngine.findJob(this.name, runnableJob.uniqueID);
    }

	/**
	 * Returns the total number of queued jobs
	 * @return
	 * 		The total number of queued jobs
	 */
	public final int size() {
		return low.size() + mid.size() + high.size();
	}

	/**
	 * Removes a job from the queue
	 * @param job
	 * 		The job to remove
	 * @return
	 * 		true if the job was in the queue and successfully removed,
	 * 		false otherwise
	 */
	public final boolean remove(Job job) {
		if(job == null)
			throw new IllegalArgumentException("Null Value");

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
            RunnableJob runnableJob = new RunnableJob(uniqueId, 0);
            return fetchJob(runnableJob);
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

    public final Packet getStatus() {
       /* StringBuilder sb = new StringBuilder();
        sb.append(this.name.toString(GearmanConstants.CHARSET)); sb.append('\t');
        sb.append(this.allJobs.size()); sb.append('\t');
        sb.append(this.allJobs.size()-this.jobsInQueue.intValue());sb.append('\t');
        sb.append(this.workers.size());sb.append('\n');

        return Packet.createTEXT(sb.toString());
       */
        return null;
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

    public Collection<Job> getAllJobs() {
        return persistenceEngine.getAllForFunction(this.name);
    }

    public HashMap<String, ImmutableList<RunnableJob>> getCopyOfJobQueues()
    {
        HashMap<String, ImmutableList<RunnableJob>> queues = new HashMap<>();
        queues.put("high", ImmutableList.copyOf(high));
        queues.put("mid",  ImmutableList.copyOf(mid));
        queues.put("low",  ImmutableList.copyOf(low));
        return queues;
    }


}
