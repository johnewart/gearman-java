package net.johnewart.gearman.server;

import com.google.common.collect.ImmutableList;
import net.johnewart.gearman.server.factories.JobFactory;
import net.johnewart.gearman.server.storage.JobQueue;
import net.johnewart.gearman.server.core.QueuedJob;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.Seconds;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class JobQueueTest {
    private JobQueue jobQueue;
    private final String queueName = "delayedJobs";

    public JobQueueTest()
    {

    }

    @Before
    public void initialize()
    {
        jobQueue = new JobQueue(queueName);
    }

    @Test
    public void doesNotRunEpochJobsEarly() throws Exception
    {
        int futureSeconds = 40;
        QueuedJob futureJob = new QueuedJob(JobFactory.generateFutureJob(queueName, Seconds.seconds(futureSeconds)));
        QueuedJob normalPriorityJob = new QueuedJob(JobFactory.generateBackgroundJob(queueName));

        jobQueue.enqueue(futureJob);

        assertThat("Job Queue has one job",
                jobQueue.size(),
                is(1));

        long currentMilliseconds = new DateTime().toDate().getTime();
        DateTimeUtils.setCurrentMillisFixed(currentMilliseconds);
        assertNull("No job is polled yet",
                jobQueue.poll());

        jobQueue.enqueue(normalPriorityJob);
        assertThat("After enqueuing a normal job, even after a future one, that it comes out first",
                jobQueue.poll(),
                is(normalPriorityJob));

        // Jump forward 10 seconds before futureSeconds
        DateTimeUtils.setCurrentMillisFixed(currentMilliseconds + ((futureSeconds - 10) * 1000));
        assertNull("No job is polled yet when time < job time",
                jobQueue.poll());

        // Jump forward 10 seconds beyond futureSeconds
        DateTimeUtils.setCurrentMillisFixed(currentMilliseconds + ((futureSeconds + 10) * 1000));
        assertThat("Future job is polled from queue after the time has come",
                jobQueue.poll(),
                is(futureJob));

    }

    @Test
    public void runsJobsInPriorityOrder() throws Exception
    {
        QueuedJob highPriorityJob = new QueuedJob(JobFactory.generateHighPriorityBackgroundJob(queueName));
        QueuedJob normalPriorityJob = new QueuedJob(JobFactory.generateBackgroundJob(queueName));
        QueuedJob lowPriorityJob = new QueuedJob(JobFactory.generateLowPriorityBackgroundJob(queueName));

        jobQueue.enqueue(lowPriorityJob);
        jobQueue.enqueue(normalPriorityJob);
        jobQueue.enqueue(highPriorityJob);

        assertThat("Job queue has three jobs",
                jobQueue.size(),
                is(3));

        assertThat("First polled job is the high priority one",
                jobQueue.poll(),
                is(highPriorityJob));

        assertThat("Second polled job is the normal priority job",
                jobQueue.poll(),
                is(normalPriorityJob));

        assertThat("Third polled job is the low priority job",
                jobQueue.poll(),
                is(lowPriorityJob));

        assertThat("Job queue has size == 0",
                jobQueue.size(),
                is(0));

        assertNull("No job is polled when size == 0",
                jobQueue.poll());
    }

    @Test
    public void doesNotDoubleEnqueueJobs() throws Exception
    {
        QueuedJob normalJob = new QueuedJob(JobFactory.generateBackgroundJob(queueName));

        jobQueue.enqueue(normalJob);
        jobQueue.enqueue(normalJob);

        assertThat("There is only one job in the queue",
                jobQueue.size(),
                is(1));

        assertThat("Polled job is the enqueued job",
                jobQueue.poll(),
                is(normalJob));

        assertThat("Job queue has size == 0",
                jobQueue.size(),
                is(0));

        assertNull("No job is polled when size == 0",
                jobQueue.poll());
    }

    @Test
    public void correctlyReportsWhenUniqueIdInUse() throws Exception {
        QueuedJob normalJob = new QueuedJob(JobFactory.generateBackgroundJob(queueName));
        jobQueue.enqueue(normalJob);
        assertThat("The queue reports that the job's unique id is in use",
                jobQueue.uniqueIdInUse(normalJob.getUniqueID()),
                is(true));

    }


    @Test
    public void enqueuedJobsAreReturned() throws Exception {
        QueuedJob job;

        for(int i = 0; i < 10; i++)
        {
            job = new QueuedJob(JobFactory.generateBackgroundJob(queueName));
            jobQueue.enqueue(job);
        }

        for(int i = 0; i < 10; i++)
        {
            job = new QueuedJob(JobFactory.generateHighPriorityBackgroundJob(queueName));
            jobQueue.enqueue(job);
        }

        for(int i = 0; i < 10; i++)
        {
            job = new QueuedJob(JobFactory.generateLowPriorityBackgroundJob(queueName));
            jobQueue.enqueue(job);
        }

        Collection<QueuedJob> allJobs = jobQueue.getAllJobs();

        assertThat("There are thirty jobs in the job queue as reported by getAllJobs()",
                allJobs.size(),
                is(30));

    }

    @Test
    public void copyOfJobQueuesContainsAllJobQueues() throws Exception
    {
        QueuedJob job;

        for(int i = 0; i < 10; i++)
        {
            job = new QueuedJob(JobFactory.generateBackgroundJob(queueName));
            jobQueue.enqueue(job);
        }

        for(int i = 0; i < 10; i++)
        {
            job = new QueuedJob(JobFactory.generateHighPriorityBackgroundJob(queueName));
            jobQueue.enqueue(job);
        }

        for(int i = 0; i < 10; i++)
        {
            job = new QueuedJob(JobFactory.generateLowPriorityBackgroundJob(queueName));
            jobQueue.enqueue(job);
        }

        HashMap<String, ImmutableList<QueuedJob>> jobMap = jobQueue.getCopyOfJobQueues();

        assertThat("There are three keys",
                jobMap.keySet().size(),
                is(3));


        assertThat("The map has the jobs for 'high'",
                jobMap.containsKey("high"),
                is(true));

        assertThat("The map has the jobs for 'mid'",
                jobMap.containsKey("mid"),
                is(true));

        assertThat("The map has the jobs for 'low'",
                jobMap.containsKey("low"),
                is(true));

        assertThat("The map has ten jobs for 'high'",
                jobMap.get("high").size(),
                is(10));

        assertThat("The map has ten jobs for 'mid'",
                jobMap.get("mid").size(),
                is(10));

        assertThat("The map has ten jobs for 'low'",
                jobMap.get("low").size(),
                is(10));

    }

    @Test
    public void removalBehavesCorrectly() throws Exception
    {
        QueuedJob normalJob = new QueuedJob(JobFactory.generateBackgroundJob(queueName));
        QueuedJob highJob = new QueuedJob(JobFactory.generateHighPriorityBackgroundJob(queueName));
        QueuedJob lowJob = new QueuedJob(JobFactory.generateLowPriorityBackgroundJob(queueName));

        jobQueue.enqueue(normalJob);
        jobQueue.enqueue(highJob);
        jobQueue.enqueue(lowJob);

        assertThat("There are three jobs in the queue",
                jobQueue.size(),
                is(3));

        assertTrue(jobQueue.remove(highJob));

        assertThat("Job queue has size == 2",
                jobQueue.size(),
                is(2));

        assertFalse(jobQueue.remove(highJob));

        assertThat("Job queue has size == 2",
                jobQueue.size(),
                is(2));

        assertTrue(jobQueue.remove(normalJob));


        assertThat("Job queue has size == 1",
                jobQueue.size(),
                is(1));

        jobQueue.remove(lowJob);

        assertThat("Job queue has size == 0",
                jobQueue.size(),
                is(0));

        assertFalse("Removing a null job fails",
                jobQueue.remove(null));
    }

}
