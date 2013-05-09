package org.gearman.server;

import org.gearman.common.Job;
import org.gearman.common.JobStatus;
import org.gearman.common.interfaces.Client;
import org.gearman.common.interfaces.Worker;
import org.gearman.server.core.*;
import org.gearman.server.factories.JobFactory;
import org.gearman.server.persistence.MemoryQueue;
import org.gearman.server.storage.JobQueue;
import org.gearman.server.storage.JobStore;
import org.jboss.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class JobStoreTest {
    private JobStore jobStore;
    private MemoryQueue memoryQueue;
    private NetworkWorker worker;

    public JobStoreTest()
    {
    }

    @Before
    public void initialize() {
        memoryQueue = new MemoryQueue();
        jobStore = new JobStore(memoryQueue);

        worker = new NetworkWorker(mock(Channel.class));
        worker.addAbility("reverseString");
        worker.addAbility("computeBigStuff");
    }

    @Test
    public void insertsJobsIntoJobStore() throws Exception {
        Job job = JobFactory.generateForegroundJob("reverseString");
        jobStore.storeJob(job);

        assertThat("There are no completed jobs",
                jobStore.getCompletedJobsCounter().count(),
                is(0L));

        assertThat("There is one pending job",
                jobStore.getPendingJobsCounter().count(),
                is(1L));

        assertThat("One job has been enqueued",
                jobStore.getQueuedJobsCounter().count(),
                is(1L));
    }

    @Test
    public void fetchesJobsFromStorage() throws Exception {

        Job job = JobFactory.generateForegroundJob("reverseString");
        jobStore.storeJob(job);

        Job nextJob = jobStore.nextJobForWorker(worker);

        assertThat("The jobs are the same",
                job.equals(nextJob),
                is(true));

        assertThat("The job store is now empty",
                jobStore.getPendingJobsCounter().count(),
                is(0L));

        assertThat("There is one active job",
                jobStore.getActiveJobsCounter().count(),
                is(1L));

        assertThat("The job store has no complete jobs",
                jobStore.getCompletedJobsCounter().count(),
                is(0L));

        // Complete the job
        jobStore.workComplete(nextJob, worker);

        assertThat("The job store has one complete job",
                jobStore.getCompletedJobsCounter().count(),
                is(1L));

    }

    @Test
    public void reQueuesBackgroundJobsWhenWorkersDisconnect() throws Exception {
        Job job = JobFactory.generateBackgroundJob("reverseString");
        jobStore.storeJob(job);

        jobStore.nextJobForWorker(worker);

        assertThat("Job queue has no jobs",
                jobStore.getJobQueue("reverseString").size(),
                is(0));

        // Simulate abort before completion
        jobStore.unregisterWorker(worker);

        assertThat("Job queue has one job",
                jobStore.getJobQueue("reverseString").size(),
                is(1));

        // Re-fetch the job, make sure it's the same one
        Job nextJob = jobStore.nextJobForWorker(worker);

        assertThat("The job was returned to the queue",
                nextJob.equals(job),
                is(true));
    }

    @Test
    public void reQueuesForegroundJobWhenClientConnected() throws Exception
    {
        Client mockClient = mock(NetworkClient.class);

        Job job = JobFactory.generateForegroundJob("reverseString");
        job.addClient(mockClient);
        jobStore.storeJob(job);

        jobStore.nextJobForWorker(worker);

        assertThat("Job queue has no jobs",
                jobStore.getJobQueue("reverseString").size(),
                is(0));

        // Simulate abort before completion
        jobStore.unregisterWorker(worker);

        assertThat("Job queue has one job",
                jobStore.getJobQueue("reverseString").size(),
                is(1));

        // Re-fetch the job, make sure it's the same one
        Job nextJob = jobStore.nextJobForWorker(worker);

        assertThat("The job was returned to the queue",
                nextJob.equals(job),
                is(true));
    }

    @Test
    public void dropsForegroundJobWhenNoClientAttached() throws Exception
    {
        Client mockClient = mock(NetworkClient.class);

        Job job = JobFactory.generateForegroundJob("reverseString");
        job.addClient(mockClient);
        jobStore.storeJob(job);

        jobStore.nextJobForWorker(worker);

        assertThat("Job queue has no jobs",
                jobStore.getJobQueue("reverseString").size(),
                is(0));

        // Simulate abort before completion
        jobStore.unregisterWorker(worker);

        assertThat("Job queue has one job",
                jobStore.getJobQueue("reverseString").size(),
                is(1));

        // Re-fetch the job, make sure it's the same one
        Job nextJob = jobStore.nextJobForWorker(worker);

        assertThat("The job was returned to the queue",
                nextJob.equals(job),
                is(true));
    }

    @Test
    public void wakesUpWorkerWhenJobComesIn() throws Exception
    {
        Job job = JobFactory.generateBackgroundJob("reverseString");
        Worker spyWorker = spy(worker);
        jobStore.registerWorkerAbility("reverseString", spyWorker);
        jobStore.sleepingWorker(spyWorker);
        jobStore.storeJob(job);
        verify(spyWorker).wakeUp();
    }

    @Test
    public void checksAndUpdatesJobStatus() throws Exception
    {
        Job job = JobFactory.generateBackgroundJob("reverseString");
        jobStore.storeJob(job);

        JobStatus jobStatus = jobStore.checkJobStatus(job.getJobHandle());

        assertThat("Job status denominator is 0",
                jobStatus.getDenominator(),
                is(0));

        assertThat("Job status numerator is 0",
                jobStatus.getNumerator(),
                is(0));

        assertThat("Job is not running",
                jobStatus.isRunning(),
                is(false));

        assertThat("Job status is unknown yet",
                jobStatus.isStatusKnown(),
                is(false));

        jobStore.nextJobForWorker(worker);

        jobStatus = jobStore.checkJobStatus(job.getJobHandle());

        assertThat("Job is running",
                jobStatus.isRunning(),
                is(true));

        assertThat("Job status is still not known",
                jobStatus.isStatusKnown(),
                is(false));

        jobStore.updateJobStatus(job.getJobHandle(), 5, 100);

        jobStatus = jobStore.checkJobStatus(job.getJobHandle());

        assertThat("Job status denominator is 100",
                jobStatus.getDenominator(),
                is(100));

        assertThat("Job status numerator is 5",
                jobStatus.getNumerator(),
                is(5));

        assertThat("Job is running",
                jobStatus.isRunning(),
                is(true));

        assertThat("Job status is known",
                jobStatus.isStatusKnown(),
                is(true));


    }

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
                memoryQueue.write(currentJob);
                allJobs.add(new QueuedJob(currentJob));
            }
        }

        Set<QueuedJob> jobsInPersistentStorage = new HashSet(memoryQueue.readAll());

        assertThat("The jobs in the persistent engine match what was put in",
                allJobs.equals(jobsInPersistentStorage),
                is(true));

        assertThat("There were 300 jobs put in the storage engine",
                allJobs.size(),
                is(300));

        assertThat("There are 300 jobs read from the storage engine",
                jobsInPersistentStorage.size(),
                is(300));

        jobStore.loadAllJobs();

        Map<String, JobQueue> jobQueueMap = jobStore.getJobQueues();

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
                jobStore.getPendingJobsCounter().count(),
                is(300L));
    }

}
