package net.johnewart.gearman.engine.queue;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.exceptions.PersistenceException;
import net.johnewart.gearman.engine.exceptions.QueueFullException;
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

import static com.codahale.metrics.MetricRegistry.name;

public class PersistedJobQueue implements JobQueue {
    private final Logger LOG = LoggerFactory.getLogger(PersistedJobQueue.class);

    private final PersistenceEngine persistenceEngine;
    private final String functionName;

    private final BlockingDeque<QueuedJob> low		= new LinkedBlockingDeque<>();
    private final BlockingDeque<QueuedJob> mid		= new LinkedBlockingDeque<>();
    private final BlockingDeque<QueuedJob> high		= new LinkedBlockingDeque<>();
    private final PriorityBlockingQueue<QueuedJob> futureJobs = new PriorityBlockingQueue<>();

    // Set of unique IDs in this queue
    private final ConcurrentHashSet<String> allJobs = new ConcurrentHashSet<>();

    private final AtomicInteger maxQueueSize;
    private final Counter highCounter, midCounter, lowCounter, totalCounter;

    public PersistedJobQueue(final String functionName,
                             final PersistenceEngine persistenceEngine,
                             final MetricRegistry metricRegistry)
    {
        this.functionName = functionName;
        this.persistenceEngine = persistenceEngine;
        this.maxQueueSize = new AtomicInteger(Integer.MAX_VALUE);

        this.highCounter = metricRegistry.counter(name("queue", metricName(), "high"));
        this.midCounter = metricRegistry.counter(name("queue", metricName(), "mid"));
        this.lowCounter = metricRegistry.counter(name("queue", metricName(), "low"));
        this.totalCounter = metricRegistry.counter(name("queue", metricName(), "total"));

        if(persistenceEngine != null)
        {
            Collection<QueuedJob> jobs = persistenceEngine.getAllForFunction(functionName);
            jobs.forEach(this::add);
        }
    }

    private boolean add(final QueuedJob queuedJob)
    {
        synchronized (this.allJobs) {
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
                    incrementCounters(queuedJob.getPriority());
                }
                return enqueued;
            }
        }
    }

    private void decrementCounters(JobPriority priority)
    {
        totalCounter.dec();
        switch(priority) {
            case LOW:
                lowCounter.dec();
                break;
            case NORMAL:
                midCounter.dec();
                break;
            case HIGH:
                highCounter.dec();
                break;
        }
    }

    private void incrementCounters(JobPriority priority)
    {
        totalCounter.inc();
        switch(priority) {
            case LOW:
                lowCounter.inc();
                break;
            case NORMAL:
                midCounter.inc();
                break;
            case HIGH:
                highCounter.inc();
                break;
        }
    }

    @Override
    public final void enqueue(final Job job) throws QueueFullException, PersistenceException
    {
        LOG.debug("Enqueueing " + job.toString());

        QueuedJob queuedJob = new QueuedJob(job);

        if(this.allJobs.size() >= maxQueueSize.intValue()) {
            throw new QueueFullException();
        }

        if(persistenceEngine.write(job)) {
            add(queuedJob);
        } else {
            // ! written to persistent store
            LOG.error("Unable to save job to persistent store");
            throw new PersistenceException("Unable to save job to persistent store");
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

            decrementCounters(job.getPriority());

            return job;
        }

        return null;
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
    public void setCapacity(final int size) {
        maxQueueSize.set(size);
    }

    @Override
    public final String getName() {
        return this.functionName;
    }

    @Override
    public long size()
    {
        return totalCounter.getCount();
    }

    @Override
    public long size(JobPriority priority)
    {
        switch(priority)
        {
            case LOW:
                return lowCounter.getCount();
            case NORMAL:
                return midCounter.getCount();
            case HIGH:
                return highCounter.getCount();
            default:
                return -1;
        }
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

            // Remove from persistence engine
            persistenceEngine.delete(job);
        }

        if (removed) {
            decrementCounters(job.getPriority());
        }

        return removed;
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
        return persistenceEngine.findJob(this.functionName, uniqueID);
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

    private String metricName() {
        return this.functionName.replaceAll(":", ".");
    }

}