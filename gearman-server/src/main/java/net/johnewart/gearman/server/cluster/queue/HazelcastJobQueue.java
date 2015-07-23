package net.johnewart.gearman.server.cluster.queue;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.queue.JobQueue;
import net.johnewart.gearman.server.cluster.core.HazelcastJob;

import java.util.Collection;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;

public class HazelcastJobQueue implements JobQueue {
    private final HazelcastInstance hazelcast;
    private final Queue<HazelcastJob> highQueue;
    private final Queue<HazelcastJob> midQueue;
    private final Queue<HazelcastJob> lowQueue;
    private final Set<String> uniqueIds;
    private final String queueName;
    private final IAtomicLong maxQueueSize;
    private final ConcurrentHashMap<String, Job> localJobs;

    public HazelcastJobQueue(String name, HazelcastInstance hazelcast) {
        this.hazelcast = hazelcast;
        this.queueName = name;
        this.highQueue = hazelcast.getQueue(format("jobs-%s-high", name));
        this.midQueue = hazelcast.getQueue(format("jobs-%s-mid", name));
        this.lowQueue = hazelcast.getQueue(format("jobs-%s-low", name));
        this.uniqueIds = hazelcast.getSet(format("uniqueids-%s", name));
        this.maxQueueSize = hazelcast.getAtomicLong(format("maxsize-%s", name));
        this.localJobs = new ConcurrentHashMap<>();
    }

    @Override
    public void enqueue(Job job) {
        localJobs.put(job.getUniqueID(), job);
        uniqueIds.add(job.getUniqueID());

        switch(job.getPriority()) {
            case HIGH:
                highQueue.add(new HazelcastJob(job));
            case NORMAL:
                midQueue.add(new HazelcastJob(job));
            case LOW:
                lowQueue.add(new HazelcastJob(job));
            default:
                break;
        }
    }

    @Override
    public int size(JobPriority priority)
    {
        switch(priority) {
            case HIGH:
                return highQueue.size();
            case NORMAL:
                return midQueue.size();
            case LOW:
                return lowQueue.size();
            default:
                return -1;
        }
    }

    @Override
    public Job poll() {
        HazelcastJob dequeued = highQueue.poll();

        if(dequeued == null) {
            dequeued = midQueue.poll();
        }

        if (dequeued == null) {
            dequeued = lowQueue.poll();
        }

        if (dequeued != null) {
            Job job = dequeued.toJob();
            localJobs.put(job.getUniqueID(), job);
            return job;
        } else {
            return null;
        }
    }

    @Override
    public int size() {
        return highQueue.size() + midQueue.size() + lowQueue.size();
    }

    @Override
    public boolean uniqueIdInUse(String uniqueID) {
        return uniqueIds.contains(uniqueID);
    }

    @Override
    public boolean isEmpty() {
        return highQueue.isEmpty() && midQueue.isEmpty() && lowQueue.isEmpty();
    }

    @Override
    public void setCapacity(int size) {
        maxQueueSize.set(size);
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public boolean remove(Job job) {
        localJobs.remove(job.getUniqueID());
        uniqueIds.remove(job.getUniqueID());

        switch(job.getPriority()) {
            case HIGH:
                return highQueue.remove(new HazelcastJob(job));
            case NORMAL:
                return midQueue.remove(new HazelcastJob(job));
            case LOW:
                return lowQueue.remove(new HazelcastJob(job));
            default:
                return false;
        }
    }

    @Override
    public Collection<QueuedJob> getAllJobs() {
        return new HashSet<>();
    }

    @Override
    public Job findJobByUniqueId(String uniqueID) {
        return localJobs.get(uniqueID);
    }

    @Override
    public ImmutableMap<Integer, Long> futureCounts() {
        return ImmutableMap.of();
    }

}
