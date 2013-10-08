package net.johnewart.gearman.engine.queue;

import com.google.common.collect.ImmutableMap;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.engine.core.QueuedJob;

import java.util.Collection;

public interface JobQueue {

    boolean enqueue(Job job);

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

    boolean remove(Job job);

    String metricName();

    Collection<QueuedJob> getAllJobs();

    Job findJobByUniqueId(String uniqueID);

    ImmutableMap<Integer, Long> futureCounts();

}
