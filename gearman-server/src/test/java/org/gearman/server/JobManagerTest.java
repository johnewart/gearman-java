package org.gearman.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.gearman.common.Job;
import org.gearman.common.JobStatus;
import org.gearman.common.interfaces.Client;
import org.gearman.common.interfaces.Worker;
import org.gearman.server.core.*;
import org.gearman.server.factories.JobFactory;
import org.gearman.server.persistence.MemoryQueue;
import org.gearman.server.storage.JobQueue;
import org.gearman.server.storage.JobManager;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class JobManagerTest {
    private JobManager jobManager;
    private MemoryQueue memoryQueue;
    private NetworkWorker worker;

    public JobManagerTest()
    {
    }

    @Before
    public void initialize() {
        memoryQueue = new MemoryQueue();
        jobManager = new JobManager(memoryQueue);

        worker = new NetworkWorker(mock(Channel.class));
        worker.addAbility("reverseString");
        worker.addAbility("computeBigStuff");
    }

    @Test
    public void insertsJobsIntoJobStore() throws Exception {
        Job job = JobFactory.generateForegroundJob("reverseString");
        jobManager.storeJob(job);

        assertThat("There are no completed jobs",
                jobManager.getCompletedJobsCounter().count(),
                is(0L));

        assertThat("There is one pending job",
                jobManager.getPendingJobsCounter().count(),
                is(1L));

        assertThat("One job has been enqueued",
                jobManager.getQueuedJobsCounter().count(),
                is(1L));
    }

    @Test
    public void fetchesJobsFromStorage() throws Exception {

        Job job = JobFactory.generateForegroundJob("reverseString");
        jobManager.storeJob(job);

        Job nextJob = jobManager.nextJobForWorker(worker);
        byte[] result = {'r','e','s','u','l','t'};

        assertThat("The jobs are the same",
                job.equals(nextJob),
                is(true));

        assertThat("The job store is now empty",
                jobManager.getPendingJobsCounter().count(),
                is(0L));

        assertThat("There is one active job",
                jobManager.getActiveJobsCounter().count(),
                is(1L));

        assertThat("The job store has no complete jobs",
                jobManager.getCompletedJobsCounter().count(),
                is(0L));

        // Complete the job
        jobManager.workComplete(nextJob, result);

        assertThat("The job store has one complete job",
                jobManager.getCompletedJobsCounter().count(),
                is(1L));

    }

    @Test
    public void reQueuesBackgroundJobsWhenWorkersDisconnect() throws Exception {
        Job job = JobFactory.generateBackgroundJob("reverseString");
        jobManager.storeJob(job);

        jobManager.nextJobForWorker(worker);

        assertThat("Job queue has no jobs",
                jobManager.getJobQueue("reverseString").size(),
                is(0));

        // Simulate abort before completion
        jobManager.unregisterWorker(worker);

        assertThat("Job queue has one job",
                jobManager.getJobQueue("reverseString").size(),
                is(1));

        // Re-fetch the job, make sure it's the same one
        Job nextJob = jobManager.nextJobForWorker(worker);

        assertThat("The job was returned to the queue",
                nextJob.equals(job),
                is(true));
    }

    @Test
    public void reQueuesForegroundJobWhenClientConnected() throws Exception
    {
        Client mockClient = mock(NetworkClient.class);

        Job job = JobFactory.generateForegroundJob("reverseString");
        jobManager.storeJobForClient(job, mockClient);

        jobManager.nextJobForWorker(worker);

        assertThat("Job queue has no jobs",
                jobManager.getJobQueue("reverseString").size(),
                is(0));

        // Simulate abort before completion
        jobManager.unregisterWorker(worker);

        assertThat("Job queue has one job",
                jobManager.getJobQueue("reverseString").size(),
                is(1));

        // Re-fetch the job, make sure it's the same one
        Job nextJob = jobManager.nextJobForWorker(worker);

        assertThat("The job was returned to the queue",
                nextJob.equals(job),
                is(true));
    }

    @Test
    public void dropsForegroundJobWhenNoClientAttached() throws Exception
    {
        Job job = JobFactory.generateForegroundJob("reverseString");
        jobManager.storeJob(job);

        jobManager.nextJobForWorker(worker);

        assertThat("Job queue has no jobs",
                jobManager.getJobQueue("reverseString").size(),
                is(0));

        // Simulate abort before completion
        jobManager.unregisterWorker(worker);

        assertThat("Job queue has no jobs",
                jobManager.getJobQueue("reverseString").size(),
                is(0));

    }

    @Test
    public void wakesUpWorkerWhenJobComesIn() throws Exception
    {
        Job job = JobFactory.generateBackgroundJob("reverseString");
        Worker spyWorker = spy(worker);
        jobManager.registerWorkerAbility("reverseString", spyWorker);
        jobManager.sleepingWorker(spyWorker);
        jobManager.storeJob(job);
        verify(spyWorker).wakeUp();
    }

    @Test
    public void handlesExceptionsWhenWakingWorkers() throws Exception
    {
        Job job = JobFactory.generateBackgroundJob("reverseString");
        Worker mockWorker = mock(Worker.class);
        //when(mockWorker.wakeUp()).thenThrow(new Exception("Can't send that packet"));
        //jobManager.registerWorkerAbility("reverseString", spyWorker);
        //jobManager.sleepingWorker(spyWorker);
        //jobManager.storeJob(job);
        //verify(spyWorker).wakeUp();
    }



    @Test
    public void checksAndUpdatesJobStatus() throws Exception
    {
        Job job = JobFactory.generateBackgroundJob("reverseString");
        jobManager.storeJob(job);

        JobStatus jobStatus = jobManager.checkJobStatus(job.getJobHandle());

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

        jobManager.nextJobForWorker(worker);

        jobStatus = jobManager.checkJobStatus(job.getJobHandle());

        assertThat("Job is running",
                jobStatus.isRunning(),
                is(true));

        assertThat("Job status is still not known",
                jobStatus.isStatusKnown(),
                is(false));

        jobManager.updateJobStatus(job.getJobHandle(), 5, 100);

        jobStatus = jobManager.checkJobStatus(job.getJobHandle());

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

        jobManager.loadAllJobs();

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
    }

    @Test
    public void coalescesResultsForMultipleClients() throws Exception {

        Client mockClientOne = mock(NetworkClient.class);
        Client mockClientTwo = mock(NetworkClient.class);

        Job jobOne = JobFactory.generateForegroundJob("reverseString");
        Job jobTwo = JobFactory.generateForegroundJob("reverseString");
        jobTwo.setUniqueID(jobOne.getUniqueID());

        jobManager.storeJobForClient(jobOne, mockClientOne);
        jobManager.storeJobForClient(jobTwo, mockClientTwo);

        assertThat("Job queue has one job because they had the same unique id",
                jobManager.getJobQueue("reverseString").size(),
                is(1));

        assertThat("There is 1 job pending in the job store",
                jobManager.getPendingJobsCounter().count(),
                is(1L));

        assertThat("There has been 1 job queued in the job store",
                jobManager.getQueuedJobsCounter().count(),
                is(1L));

        Job nextJob = jobManager.nextJobForWorker(worker);
        byte[] result = {'r','e','s','u','l','t'};

        assertThat("Job pulled out is equal to job #1",
                nextJob.equals(jobOne),
                is(true));

        // Complete the job
        jobManager.workComplete(nextJob, result);

        verify(mockClientOne).sendWorkResults(jobOne.getJobHandle(), result);
        verify(mockClientTwo).sendWorkResults(jobOne.getJobHandle(), result);
    }

    // TODO: Verify that it will coalesce results if a job is submitted while a worker is working on the same one

    @Test
    public void sendsWorkDataResultsToClients() throws Exception {

        Client mockClient = mock(NetworkClient.class);

        Job jobOne = JobFactory.generateForegroundJob("reverseString");

        jobManager.storeJobForClient(jobOne, mockClient);

        Job nextJob = jobManager.nextJobForWorker(worker);
        byte[] data = {'r','e','s','u','l','t'};

        // Send back some data
        jobManager.workData(nextJob, data);

        verify(mockClient).sendWorkData(jobOne.getJobHandle(), data);
    }

}
