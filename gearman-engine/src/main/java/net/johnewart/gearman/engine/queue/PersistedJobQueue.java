package net.johnewart.gearman.engine.queue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.queue.persistence.PersistenceEngine;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PersistedJobQueue implements JobQueue {
    private final Logger LOG = LoggerFactory.getLogger(PersistedJobQueue.class);

    private final PersistenceEngine persistenceEngine;
    private final String name;

    private final BlockingDeque<QueuedJob> low		= new LinkedBlockingDeque<>();
    private final BlockingDeque<QueuedJob> mid		= new LinkedBlockingDeque<>();
    private final BlockingDeque<QueuedJob> high		= new LinkedBlockingDeque<>();
    private final PriorityBlockingQueue<QueuedJob> futureJobs = new PriorityBlockingQueue<>();

    // Set of unique IDs in this queue
    private final ConcurrentHashSet<String> allJobs = new ConcurrentHashSet<>();

    private final AtomicInteger maxQueueSize;

    // Make size() a cheaper operation.
    private final AtomicLong lowPriorityCount;
    private final AtomicLong midPriorityCount;
    private final AtomicLong highPriorityCount;

    public PersistedJobQueue(final String name, final PersistenceEngine persistenceEngine) {
        this.name = name;
        this.persistenceEngine = persistenceEngine;
        lowPriorityCount = new AtomicLong();
        midPriorityCount = new AtomicLong();
        highPriorityCount = new AtomicLong();
        maxQueueSize = new AtomicInteger(0);
    }


    @Override
    public boolean add(final QueuedJob queuedJob) {
        synchronized (this.allJobs) {

            if(this.maxQueueSize.intValue() > 0 && maxQueueSize.intValue() <= size()) {
                return false;
            }

            if(allJobs.contains(queuedJob.getUniqueID()))
            {
                return false;
            } else {
                final boolean enqueued;
                allJobs.add(queuedJob.getUniqueID());

                switch (queuedJob.getPriority()) {
                    case LOW:
                        enqueued = low.add(queuedJob);
                        break;
                    case NORMAL:
                        // Future job, stick on future queue
                        if(queuedJob.getTimeToRun() > 0) {
                            enqueued = futureJobs.add(queuedJob);
                        } else {
                            enqueued = mid.add(queuedJob);
                        }
                        break;
                    case HIGH:
                        enqueued = high.add(queuedJob);
                        break;
                    default:
                        enqueued = false;
                }

                if (enqueued) {
                    incrementJobCounter(queuedJob.getPriority());
                }

                return enqueued;
            }
        }
    }

    @Override
    public final boolean enqueue(final Job job) {
        LOG.debug("Enqueueing " + job.toString());

        QueuedJob queuedJob = new QueuedJob(job);

        if(persistenceEngine.write(job)) {
            return add(queuedJob);
        } else {
            // ! written to persistent store
            LOG.error("Unable to save job to persistent store");
            return false;
        }

    }

    /**
     * Fetch the next job waiting -- this checks high, then normal, then low
     * Caveat: in the normal queue, we skip over any jobs whose timestamp has not
     * come yet (support for epoch jobs)
     *
     * Removes the job from the queue.
     *
     * @return Next Job in the queue, null if none
     */
    @Override
    public final Job poll() {

        long currentTime = new DateTime().toDate().getTime() / 1000;

        // High has no epoch jobs
        QueuedJob queuedJob = high.poll();

        if (queuedJob == null)
        {
            // Check future jobs
            QueuedJob peekJob = futureJobs.peek();
            if (peekJob != null && peekJob.getTimeToRun() < currentTime) {
                queuedJob = futureJobs.poll();
            } else {
                queuedJob = mid.poll();
            }
        }

        if(queuedJob == null)
            queuedJob = low.poll();

        if (queuedJob != null)
        {
            Job job = persistenceEngine.findJob(
                    queuedJob.functionName,
                    queuedJob.uniqueID
            );

            allJobs.remove(job.getUniqueID());
            decrementJobCounter(job.getPriority());

            return job;
        }

        return null;
    }

    /**
     * Returns the total number of jobs in this queue
     * @return
     * 		The total number of jobs in all priorities
     */
    @Override
    public final long size() {
        return lowPriorityCount.longValue() + midPriorityCount.longValue() + highPriorityCount.longValue();
    }

    @Override
    public long size(final JobPriority jobPriority) {
        switch(jobPriority) {
            case LOW:
                return lowPriorityCount.longValue();
            case NORMAL:
                return midPriorityCount.longValue();
            case HIGH:
                return highPriorityCount.longValue();
            default:
                return -1;
        }
    }

    @Override
    public final boolean uniqueIdInUse(final String uniqueID) {
        return allJobs.contains(uniqueID);
    }

    @Override
    public final boolean isEmpty() {
        return high.isEmpty() && mid.isEmpty() && low.isEmpty();
    }

    @Override
    public void setMaxSize(final int size) {
        maxQueueSize.set(size);
    }

    @Override
    public final String getName() {
        return this.name;
    }

    @Override
    public boolean remove(final Job job) {
        if (job == null) {
            return false;
        }

        final boolean removed;

        synchronized (allJobs) {
            allJobs.remove(job.getUniqueID());

            switch(job.getPriority()) {
                case LOW:
                    removed = low.remove(new QueuedJob(job));
                    break;
                case HIGH:
                    removed = high.remove(new QueuedJob(job));
                    break;
                case NORMAL:
                    removed = mid.remove(new QueuedJob(job));
                    break;
                default:
                    removed = false;
            }

            if (removed) {
                persistenceEngine.delete(job);
                decrementJobCounter(job.getPriority());
            }
        }

        return removed;
    }

    @Override
    public String metricName() {
        return this.name.replaceAll(":", ".");
    }

    @Override
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

    @Override
    public Job findJobByUniqueId(String uniqueID) {
        return persistenceEngine.findJob(this.name, uniqueID);
    }

    @Override
    public ImmutableMap<Integer, Long> futureCounts() {
        // Time in seconds
        long currentTime = new Date().getTime() / 1000;
        HashMap<Integer, Long> hourCounts = new HashMap<>();

        // Initialize 0-hour
        hourCounts.put(0, 0L);

        ImmutableList<QueuedJob> midJobs = ImmutableList.copyOf(mid);
        for(QueuedJob job : midJobs)
        {
            final long timeDiff = job.timeToRun - currentTime;
            final Integer hoursFromNow;

            if(timeDiff > 0)
            {
                hoursFromNow = (int)(timeDiff / 3600);

                if(!hourCounts.containsKey(hoursFromNow))
                {
                    hourCounts.put(hoursFromNow, 0L);
                }
            } else {
                hoursFromNow = 0;
            }

            hourCounts.put(hoursFromNow, hourCounts.get(hoursFromNow) + 1);
        }

        return ImmutableMap.copyOf(hourCounts);
    }



    private void decrementJobCounter(final JobPriority jobPriority) {
        switch(jobPriority) {
            case LOW:
                lowPriorityCount.decrementAndGet();
                break;
            case HIGH:
                highPriorityCount.decrementAndGet();
                break;
            case NORMAL:
                midPriorityCount.decrementAndGet();
                break;
        }
    }

    private void incrementJobCounter(final JobPriority jobPriority) {
        switch(jobPriority) {
            case LOW:
                lowPriorityCount.incrementAndGet();
                break;
            case HIGH:
                highPriorityCount.incrementAndGet();
                break;
            case NORMAL:
                midPriorityCount.incrementAndGet();
                break;
        }
    }
}