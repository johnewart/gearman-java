package net.johnewart.gearman.engine.queue;

import com.google.common.collect.ImmutableMap;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.engine.core.QueuedJob;

import java.util.Collection;
import java.util.List;

public interface JobQueue {

    /**
     * Enqueue work
     * @param job
     * @return
     */
    boolean enqueue(Job job);
    /**
     * Remove a job from the queue - simply deleting it
     * @param job
     * @return true on success, false otherwise
     */
    boolean remove(Job job);

    /**
     * Add a queued job directly to the queue, skipping the traditional enqueue process
     * @param queuedJob
     * @return
     */
    boolean add(QueuedJob queuedJob);

    /**
     * Fetch the next job waiting -- this checks high, then normal, then low
     * Caveat: in the normal queue, we skip over any jobs whose timestamp has not
     * come yet (support for epoch jobs)
     *
     * @return Next Job in the queue, null if none
     */
	Job poll();

 	/**
	 * Returns the total number of jobs in this queue
	 * @return
	 * 		The total number of jobs in all priorities
	 */
    long size();

    /**
     * The size of a particular priority queue
     * @param jobPriority
     * @return
     */
    long size(JobPriority jobPriority);

    /**
     * Determine if the unique ID specified is in use.
     * @param uniqueID The job's unique ID
     * @return true or false.
     */
    boolean uniqueIdInUse(String uniqueID);

	boolean isEmpty();

    void setMaxSize(int size);

    String getName();

    String metricName();

    // Data
    Collection<QueuedJob> getAllJobs();

    Job findJobByUniqueId(String uniqueID);

    ImmutableMap<Integer, Long> futureCounts();

}
