package org.gearman.server.persistence;

import org.gearman.common.Job;
import org.gearman.server.factories.JobFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class MemoryQueueTest {
    private MemoryQueue memoryQueue;
    private final String[] jobQueues = {
            "delayedJobs",
            "immediateJobs",
            "gearmanJobs"
    };
    private final int jobsCount = 100;
    private Set<String> jobHandles;
    private Set<Job> allJobs;

    @Before
    public void initialize()
    {
        memoryQueue = new MemoryQueue();
        jobHandles = new HashSet<>();
        allJobs = new HashSet<>();

        Job currentJob;

        for(String jobQueue : jobQueues)
        {
            for(int i = 0; i < jobsCount; i++)
            {
                currentJob = JobFactory.generateBackgroundJob(jobQueue);
                memoryQueue.write(currentJob);
                jobHandles.add(currentJob.getJobHandle());
                allJobs.add(currentJob);
            }
        }
    }

    @Test
    public void allJobsAreInQueue() throws Exception
    {
        assertThat("There are 300 jobs in the job set",
                allJobs.size(),
                is(300));

    }

    @Test
    public void eachQueueIsTheRightSize() throws Exception
    {
        for(String jobQueue : jobQueues)
        {
            assertThat("There are " + jobsCount + " jobs in the '" + jobQueue + "' queue",
                memoryQueue.getAllForFunction(jobQueue).size(),
                is(jobsCount));
        }
    }

    @Test
    public void fetchesAllJobsByJobHandle() throws Exception
    {
        for(String jobHandle : jobHandles)
        {
            Job job = memoryQueue.findJobByHandle(jobHandle);
            allJobs.remove(job);
        }

        assertThat("All jobs have been removed from the job set",
                allJobs.size(),
                is(0));
    }

    @Test
    public void returnsNullWhenUnknownQueueFetched() throws Exception
    {
        assertThat("Unknown queue has no jobs",
                memoryQueue.getAllForFunction("nonExistentQueue").size(),
                is(0));
    }

    @Test
    public void readAllHasAllJobs() throws Exception
    {
        assertThat("Read all has 300 jobs even though it doesn't actually persist",
                memoryQueue.readAll().size(),
                is(300));
    }

    @Test
    public void deleteAllJobs() throws Exception
    {
        memoryQueue.deleteAll();

        for(String jobQueue : jobQueues)
        {
            assertThat("There are 0 jobs in the '" + jobQueue + "' queue",
                    memoryQueue.getAllForFunction(jobQueue).size(),
                    is(0));
        }
    }

    @Test
    public void deleteJobsIndividuallyByUniqueID() throws Exception
    {
        for(Job job : allJobs)
        {
            memoryQueue.delete(job.getFunctionName(), job.getUniqueID());
        }

        for(String jobQueue : jobQueues)
        {
            assertThat("There are 0 jobs in the '" + jobQueue + "' queue",
                    memoryQueue.getAllForFunction(jobQueue).size(),
                    is(0));
        }
    }

    @Test
    public void deleteJobsIndividually() throws Exception
    {
        for(Job job : allJobs)
        {
            memoryQueue.delete(job);
        }

        for(String jobQueue : jobQueues)
        {
            assertThat("There are 0 jobs in the '" + jobQueue + "' queue",
                    memoryQueue.getAllForFunction(jobQueue).size(),
                    is(0));
        }
    }

}
