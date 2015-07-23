package net.johnewart.gearman.engine.queue;

import com.google.common.collect.ImmutableMap;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.exceptions.PersistenceException;
import net.johnewart.gearman.engine.exceptions.QueueFullException;

import java.util.Collection;

public interface JobQueue {

    /**
     * Enqueue work
     * @param job
     */
    void enqueue(Job job) throws QueueFullException, PersistenceException;

    long size(JobPriority priority);

    /**
     * Remove a job from the queue - simply deleting it
     * @param job
     * @return true on success, false otherwise
     */
    boolean remove(Job job);

    /**
     * Fetch the next job waiting -- this checks high, then normal, then low
     * Caveat: in the normal queue, we skip over any jobs whose timestamp has not
     * come yet (support for epoch jobs)
     *
     * @return Next Job in the queue, null if none
     */
	Job poll();

    /**
     * Determine if the unique ID specified is in use.
     * @param uniqueID The job's unique ID
     * @return true or false.
     */
    boolean uniqueIdInUse(String uniqueID);

	boolean isEmpty();

    void setCapacity(int size);

    String getName();

    long size();

    // Data
    Collection<QueuedJob> getAllJobs();

    Job findJobByUniqueId(String uniqueID);

    ImmutableMap<Integer, Long> futureCounts();
}
