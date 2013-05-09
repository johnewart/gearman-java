package org.gearman.server;

import org.gearman.server.core.QueuedJob;
import org.gearman.server.factories.JobFactory;
import org.gearman.server.storage.JobQueue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.Seconds;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

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


}
