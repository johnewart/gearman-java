package net.johnewart.gearman.engine;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.factories.JobFactory;
import net.johnewart.gearman.engine.queue.persistence.MemoryPersistenceEngine;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

public class MemoryQueueTest {
    private MemoryPersistenceEngine memoryPersistenceEngine;
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
        memoryPersistenceEngine = new MemoryPersistenceEngine();
        jobHandles = new HashSet<>();
        allJobs = new HashSet<>();

        Job currentJob;

        for(String jobQueue : jobQueues)
        {
            for(int i = 0; i < jobsCount; i++)
            {
                currentJob = JobFactory.generateBackgroundJob(jobQueue);
                memoryPersistenceEngine.write(currentJob);
                jobHandles.add(currentJob.getJobHandle());
                allJobs.add(currentJob);
            }
        }
    }

    @Test
    public void allJobsAreInQueue() throws Exception
    {
        Assert.assertThat("There are 300 jobs in the job set",
                allJobs.size(),
                Is.is(300));

    }

    @Test
    public void eachQueueIsTheRightSize() throws Exception
    {
        for(String jobQueue : jobQueues)
        {
            Assert.assertThat("There are " + jobsCount + " jobs in the '" + jobQueue + "' queue",
                    memoryPersistenceEngine.getAllForFunction(jobQueue).size(),
                    Is.is(jobsCount));
        }
    }

   /* @Test
    public void fetchesAllJobsByJobHandle() throws Exception
    {
        for(String jobHandle : jobHandles)
        {
            Job job = memoryPersistenceEngine.findJobByHandle(jobHandle);
            allJobs.remove(job);
        }

        Assert.assertThat("All jobs have been removed from the job set",
                allJobs.size(),
                Is.is(0));
    } */

    @Test
    public void returnsNullWhenUnknownQueueFetched() throws Exception
    {
        Assert.assertThat("Unknown queue has no jobs",
                memoryPersistenceEngine.getAllForFunction("nonExistentQueue").size(),
                Is.is(0));
    }

    @Test
    public void readAllHasAllJobs() throws Exception
    {
        Assert.assertThat("Read all has 300 jobs even though it doesn't actually persist",
                memoryPersistenceEngine.readAll().size(),
                Is.is(300));
    }

    @Test
    public void deleteAllJobs() throws Exception
    {
        memoryPersistenceEngine.deleteAll();

        for(String jobQueue : jobQueues)
        {
            Assert.assertThat("There are 0 jobs in the '" + jobQueue + "' queue",
                    memoryPersistenceEngine.getAllForFunction(jobQueue).size(),
                    Is.is(0));
        }
    }

    @Test
    public void deleteJobsIndividuallyByUniqueID() throws Exception
    {
        for(Job job : allJobs)
        {
            memoryPersistenceEngine.delete(job.getFunctionName(), job.getUniqueID());
        }

        for(String jobQueue : jobQueues)
        {
            Assert.assertThat("There are 0 jobs in the '" + jobQueue + "' queue",
                    memoryPersistenceEngine.getAllForFunction(jobQueue).size(),
                    Is.is(0));
        }
    }

    @Test
    public void deleteJobsIndividually() throws Exception
    {
        for(Job job : allJobs)
        {
            memoryPersistenceEngine.delete(job);
        }

        for(String jobQueue : jobQueues)
        {
            Assert.assertThat("There are 0 jobs in the '" + jobQueue + "' queue",
                    memoryPersistenceEngine.getAllForFunction(jobQueue).size(),
                    Is.is(0));
        }
    }

    @Test
    public void dataThatGoesInComesBackOut() throws Exception
    {
        String jobQueue = jobQueues[0];
        byte[] jobData = {'b','a','r'};
        QueuedJob queuedJob = new LinkedList<>(memoryPersistenceEngine.getAllForFunction(jobQueue)).get(0);
        Job job = memoryPersistenceEngine.findJob(queuedJob.getFunctionName(), queuedJob.getUniqueID());

        Assert.assertThat("Job data is {'b','a','r'}",
                job.getData(),
                Is.is(jobData));
    }

    /*
    @Test
    public void loadQueuedJobsFromPersistenceEngine() throws Exception
    {
        final String[] jobQueueNames = {
                "delayedJobs",
                "immediateJobs",
                "gearmanJobs"
        };
        final int jobsCount = 100;
        final Set<QueuedJob> allJobs = new HashSet<>();
        Job currentJob;
        for(String jobQueueName : jobQueueNames)
        {
            for(int i = 0; i < jobsCount; i++)
            {
                currentJob = JobFactory.generateBackgroundJob(jobQueueName);
                memoryPersistenceEngine.write(currentJob);
                allJobs.add(new QueuedJob(currentJob));
            }
        }

        Set<QueuedJob> jobsInPersistentStorage = new HashSet(memoryPersistenceEngine.readAll());

        assertThat("The jobs in the persistent engine match what was put in",
                allJobs.equals(jobsInPersistentStorage),
                is(true));

        assertThat("There were 300 jobs put in the storage engine",
                allJobs.size(),
                is(300));

        assertThat("There are 300 jobs read from the storage engine",
                jobsInPersistentStorage.size(),
                is(300));

        jobManager;

        Map<String, JobQueue> jobQueueMap = jobManager.getJobQueues();

        assertThat("There are three job queues in the storage",
                jobQueueMap.keySet().size(),
                is(3));

        for(String jobQueueName : jobQueueNames)
        {
            assertThat("The job queues contains a queue named '" + jobQueueName + "'",
                    jobQueueMap.containsKey(jobQueueName),
                    is(true));
        }

        assertThat("There are 300 jobs pending in the job store",
                jobManager.getPendingJobsCounter().count(),
                is(300L));
    }*/

}
