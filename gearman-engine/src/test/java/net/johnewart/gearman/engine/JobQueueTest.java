package net.johnewart.gearman.engine;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.engine.core.QueuedJob;
import net.johnewart.gearman.engine.exceptions.JobQueueFactoryException;
import net.johnewart.gearman.engine.factories.JobFactory;
import net.johnewart.gearman.engine.queue.JobQueue;
import net.johnewart.gearman.engine.queue.factories.MemoryJobQueueFactory;
import org.hamcrest.core.Is;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.Seconds;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

public class JobQueueTest {
    private JobQueue jobQueue;
    private final String queueName = "delayedJobs";
    private final MemoryJobQueueFactory memoryJobQueueFactory;

    public JobQueueTest()
    {
        memoryJobQueueFactory = new MemoryJobQueueFactory();
    }

    @Before
    public void initialize() throws JobQueueFactoryException
    {
        jobQueue = memoryJobQueueFactory.build(queueName);
    }

    @Test
    public void doesNotRunEpochJobsEarly() throws Exception
    {
        int futureSeconds = 40;
        Job futureJob = JobFactory.generateFutureJob(queueName, Seconds.seconds(futureSeconds));
        Job normalPriorityJob = JobFactory.generateBackgroundJob(queueName);

        jobQueue.enqueue(futureJob);

        Assert.assertThat("Job Queue has one job",
                jobQueue.size(),
                Is.is(1L));

        long currentMilliseconds = new DateTime().toDate().getTime();
        DateTimeUtils.setCurrentMillisFixed(currentMilliseconds);
        Assert.assertNull("No job is polled yet",
                jobQueue.poll());

        jobQueue.enqueue(normalPriorityJob);
        Assert.assertThat("After enqueuing a normal job, even after a future one, that it comes out first",
                jobQueue.poll(),
                Is.is(normalPriorityJob));

        // Jump forward 10 seconds before futureSeconds
        DateTimeUtils.setCurrentMillisFixed(currentMilliseconds + ((futureSeconds - 10) * 1000));
        Assert.assertNull("No job is polled yet when time < job time",
                jobQueue.poll());

        // Jump forward 10 seconds beyond futureSeconds
        DateTimeUtils.setCurrentMillisFixed(currentMilliseconds + ((futureSeconds + 10) * 1000));
        Assert.assertThat("Future job is polled from queue after the time has come",
                jobQueue.poll(),
                Is.is(futureJob));

    }

    @Test
    public void runsJobsInPriorityOrder() throws Exception
    {
        Job highPriorityJob = JobFactory.generateHighPriorityBackgroundJob(queueName);
        Job normalPriorityJob = JobFactory.generateBackgroundJob(queueName);
        Job lowPriorityJob = JobFactory.generateLowPriorityBackgroundJob(queueName);

        jobQueue.enqueue(lowPriorityJob);
        jobQueue.enqueue(normalPriorityJob);
        jobQueue.enqueue(highPriorityJob);

        Assert.assertThat("Job queue has three jobs",
                jobQueue.size(),
                Is.is(3L));

        Assert.assertThat("First polled job is the high priority one",
                jobQueue.poll(),
                Is.is(highPriorityJob));

        Assert.assertThat("Second polled job is the normal priority job",
                jobQueue.poll(),
                Is.is(normalPriorityJob));

        Assert.assertThat("Third polled job is the low priority job",
                jobQueue.poll(),
                Is.is(lowPriorityJob));

        Assert.assertThat("Job queue has size == 0",
                jobQueue.size(),
                Is.is(0L));

        Assert.assertNull("No job is polled when size == 0",
                jobQueue.poll());
    }

    @Test
    public void doesNotDoubleEnqueueJobs() throws Exception
    {
        Job normalJob = JobFactory.generateBackgroundJob(queueName);

        jobQueue.enqueue(normalJob);
        jobQueue.enqueue(normalJob);

        Assert.assertThat("There is only one job in the queue",
                jobQueue.size(),
                Is.is(1L));

        Assert.assertThat("Polled job is the enqueued job",
                jobQueue.poll(),
                Is.is(normalJob));

        Assert.assertThat("Job queue has size == 0",
                jobQueue.size(),
                Is.is(0L));

        Assert.assertNull("No job is polled when size == 0",
                jobQueue.poll());
    }

    @Test
    public void correctlyReportsWhenUniqueIdInUse() throws Exception {
        Job normalJob = JobFactory.generateBackgroundJob(queueName);
        jobQueue.enqueue(normalJob);
        Assert.assertThat("The queue reports that the job's unique id is in use",
                jobQueue.uniqueIdInUse(normalJob.getUniqueID()),
                Is.is(true));

    }


    @Test
    public void enqueuedJobsAreReturned() throws Exception {
        Job job;

        for(int i = 0; i < 10; i++)
        {
            job = JobFactory.generateBackgroundJob(queueName);
            jobQueue.enqueue(job);
        }

        for(int i = 0; i < 10; i++)
        {
            job = JobFactory.generateHighPriorityBackgroundJob(queueName);
            jobQueue.enqueue(job);
        }

        for(int i = 0; i < 10; i++)
        {
            job = JobFactory.generateLowPriorityBackgroundJob(queueName);
            jobQueue.enqueue(job);
        }

        Collection<QueuedJob> allJobs = jobQueue.getAllJobs();

        Assert.assertThat("There are thirty jobs in the job queue as reported by getAllJobs()",
                allJobs.size(),
                Is.is(30));

    }

    @Test
    public void removalBehavesCorrectly() throws Exception
    {
        Job normalJob = JobFactory.generateBackgroundJob(queueName);
        Job highJob = JobFactory.generateHighPriorityBackgroundJob(queueName);
        Job lowJob = JobFactory.generateLowPriorityBackgroundJob(queueName);

        jobQueue.enqueue(normalJob);
        jobQueue.enqueue(highJob);
        jobQueue.enqueue(lowJob);

        Assert.assertThat("There are three jobs in the queue",
                jobQueue.size(),
                Is.is(3L));

        Assert.assertTrue(jobQueue.remove(highJob));

        Assert.assertThat("Job queue has size == 2",
                jobQueue.size(),
                Is.is(2L));

        Assert.assertFalse(jobQueue.remove(highJob));

        Assert.assertThat("Job queue still has size == 2",
                jobQueue.size(),
                Is.is(2L));

        Assert.assertTrue(jobQueue.remove(normalJob));


        Assert.assertThat("Job queue has size == 1",
                jobQueue.size(),
                Is.is(1L));

        jobQueue.remove(lowJob);

        Assert.assertThat("Job queue has size == 0",
                jobQueue.size(),
                Is.is(0L));

        Assert.assertFalse("Removing a null job fails",
                jobQueue.remove(null));
    }

}
