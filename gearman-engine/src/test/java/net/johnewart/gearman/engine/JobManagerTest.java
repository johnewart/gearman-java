package net.johnewart.gearman.engine;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.JobStatus;
import net.johnewart.gearman.common.interfaces.EngineClient;
import net.johnewart.gearman.common.interfaces.EngineWorker;
import net.johnewart.gearman.common.interfaces.JobHandleFactory;
import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.engine.core.UniqueIdFactory;
import net.johnewart.gearman.engine.factories.JobFactory;
import net.johnewart.gearman.engine.factories.TestJobHandleFactory;
import net.johnewart.gearman.engine.factories.TestUniqueIdFactory;
import net.johnewart.gearman.engine.metrics.MetricsEngine;
import net.johnewart.gearman.engine.queue.factories.MemoryJobQueueFactory;
import net.johnewart.gearman.engine.storage.NoopExceptionStorageEngine;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JobManagerTest {
    private JobManager jobManager;
    private EngineWorker worker;
    private JobHandleFactory jobHandleFactory;
    private UniqueIdFactory uniqueIdFactory;
    private MetricsEngine metricsEngine;

    public JobManagerTest()
    {
    }

    @Before
    public void initialize() {
        MetricRegistry metricRegistry = new MetricRegistry();
        metricsEngine = new MetricsEngine(metricRegistry);
        jobHandleFactory = new TestJobHandleFactory();
        uniqueIdFactory = new TestUniqueIdFactory();
        jobManager = new JobManager(new MemoryJobQueueFactory(metricRegistry), jobHandleFactory, uniqueIdFactory, new NoopExceptionStorageEngine(), metricsEngine);

        final ImmutableSet<String> abilities = ImmutableSet.of("reverseString", "computeBigStuff");
        worker = mock(EngineWorker.class);
        when(worker.getAbilities()).thenReturn(abilities);
    }

    @Test
    public void insertsJobsIntoJobStore() throws Exception {
        Job job = JobFactory.generateForegroundJob("reverseString");
        jobManager.storeJob(job);

        Assert.assertThat("There are no completed jobs",
                metricsEngine.getCompletedJobCount(),
                Is.is(0L));

        Assert.assertThat("There is one pending job",
                metricsEngine.getPendingJobsCount(),
                Is.is(1L));

        Assert.assertThat("One job has been enqueued",
                metricsEngine.getEnqueuedJobCount(),
                Is.is(1L));
    }

    @Test
    public void fetchesJobsFromStorage() throws Exception {

        Job job = JobFactory.generateForegroundJob("reverseString");
        jobManager.storeJob(job);

        Job nextJob = jobManager.nextJobForWorker(worker);
        byte[] result = {'r','e','s','u','l','t'};

        Assert.assertThat("The jobs are the same",
                job.equals(nextJob),
                Is.is(true));

        Assert.assertThat("The job store is now empty",
                metricsEngine.getPendingJobsCount(),
                Is.is(0L));

        Assert.assertThat("There is one active job",
                metricsEngine.getActiveJobCount(),
                Is.is(1L));

        Assert.assertThat("The job store has no complete jobs",
                metricsEngine.getCompletedJobCount(),
                Is.is(0L));

        // Complete the job
        jobManager.handleWorkCompletion(nextJob, result);

        Assert.assertThat("The job store has one complete job",
                metricsEngine.getCompletedJobCount(),
                Is.is(1L));

    }

    @Test
    public void reQueuesBackgroundJobsWhenWorkersDisconnect() throws Exception {
        Job job = JobFactory.generateBackgroundJob("reverseString");
        jobManager.storeJob(job);

        jobManager.nextJobForWorker(worker);

        Assert.assertThat("Job queue has no jobs",
                jobManager.getOrCreateJobQueue("reverseString").size(),
                Is.is(0L));

        // Simulate abort before completion
        jobManager.unregisterWorker(worker);

        Assert.assertThat("Job queue has one job",
                jobManager.getOrCreateJobQueue("reverseString").size(),
                Is.is(1L));

        // Re-fetch the job, make sure it's the same one
        Job nextJob = jobManager.nextJobForWorker(worker);

        Assert.assertThat("The job was returned to the queue",
                nextJob.equals(job),
                Is.is(true));
    }

    @Test
    public void reQueuesForegroundJobWhenClientConnected() throws Exception
    {
        EngineClient mockClient = mock(EngineClient.class);

        Job job = JobFactory.generateForegroundJob("reverseString");
        jobManager.storeJobForClient(job, mockClient);

        jobManager.nextJobForWorker(worker);

        Assert.assertThat("Job queue has no jobs",
                jobManager.getOrCreateJobQueue("reverseString").size(),
                Is.is(0L));

        // Simulate abort before completion
        jobManager.unregisterWorker(worker);

        Assert.assertThat("Job queue has one job",
                jobManager.getOrCreateJobQueue("reverseString").size(),
                Is.is(1L));

        // Re-fetch the job, make sure it's the same one
        Job nextJob = jobManager.nextJobForWorker(worker);

        Assert.assertThat("The job was returned to the queue",
                nextJob.equals(job),
                Is.is(true));
    }

    @Test
    public void dropsForegroundJobWhenNoClientAttached() throws Exception
    {
        Job job = JobFactory.generateForegroundJob("reverseString");
        jobManager.storeJob(job);

        jobManager.nextJobForWorker(worker);

        Assert.assertThat("Job queue has no jobs",
                jobManager.getOrCreateJobQueue("reverseString").size(),
                Is.is(0L));

        // Simulate abort before completion
        jobManager.unregisterWorker(worker);

        Assert.assertThat("Job queue has no jobs",
                jobManager.getOrCreateJobQueue("reverseString").size(),
                Is.is(0L));

    }

    @Test
    public void wakesUpWorkerWhenJobComesIn() throws Exception
    {
        Job job = JobFactory.generateBackgroundJob("reverseString");
        jobManager.registerWorkerAbility("removeString", worker);
        jobManager.markWorkerAsAsleep(worker);
        jobManager.storeJob(job);
        verify(worker).wakeUp();
    }

    @Test
    public void handlesExceptionsWhenWakingWorkers() throws Exception
    {
        Job job = JobFactory.generateBackgroundJob("reverseString");
        EngineWorker mockWorker = mock(EngineWorker.class);
        //when(mockWorker.wakeUp()).thenThrow(new Exception("Can't send that packet"));
        //jobManager.registerWorkerAbility("reverseString", spyWorker);
        //jobManager.markWorkerAsAsleep(spyWorker);
        //jobManager.storeJob(job);
        //verify(spyWorker).wakeUp();
    }



    @Test
    public void checksAndUpdatesJobStatus() throws Exception
    {
        Job job = JobFactory.generateBackgroundJob("reverseString");
        jobManager.storeJob(job);

        JobStatus jobStatus = jobManager.checkJobStatus(job.getJobHandle());

        Assert.assertThat("Job status denominator is 0",
                jobStatus.getDenominator(),
                Is.is(0));

        Assert.assertThat("Job status numerator is 0",
                jobStatus.getNumerator(),
                Is.is(0));

        Assert.assertThat("Job is not running",
                jobStatus.isRunning(),
                Is.is(false));

        Assert.assertThat("Job status is unknown yet",
                jobStatus.isStatusKnown(),
                Is.is(false));

        jobManager.nextJobForWorker(worker);

        jobStatus = jobManager.checkJobStatus(job.getJobHandle());

        Assert.assertThat("Job is running",
                jobStatus.isRunning(),
                Is.is(true));

        Assert.assertThat("Job status is still not known",
                jobStatus.isStatusKnown(),
                Is.is(false));

        jobManager.updateJobStatus(job.getJobHandle(), 5, 100);

        jobStatus = jobManager.checkJobStatus(job.getJobHandle());

        Assert.assertThat("Job status denominator is 100",
                jobStatus.getDenominator(),
                Is.is(100));

        Assert.assertThat("Job status numerator is 5",
                jobStatus.getNumerator(),
                Is.is(5));

        Assert.assertThat("Job is running",
                jobStatus.isRunning(),
                Is.is(true));

        Assert.assertThat("Job status is known",
                jobStatus.isStatusKnown(),
                Is.is(true));


    }

    @Test
    public void coalescesResultsForMultipleClients() throws Exception {

        EngineClient mockClientOne = mock(EngineClient.class);
        EngineClient mockClientTwo = mock(EngineClient.class);

        Job jobOne = JobFactory.generateForegroundJob("reverseString");
        Job jobTwo = JobFactory.generateForegroundJob("reverseString");
        jobTwo.setUniqueID(jobOne.getUniqueID());

        jobManager.storeJobForClient(jobOne, mockClientOne);
        jobManager.storeJobForClient(jobTwo, mockClientTwo);

        Assert.assertThat("Job queue has one job because they had the same unique id",
                jobManager.getOrCreateJobQueue("reverseString").size(),
                Is.is(1L));

        Assert.assertThat("There is 1 job pending in the job store",
                metricsEngine.getPendingJobsCount(),
                Is.is(1L));

        Assert.assertThat("There has been 1 job queued in the job store",
                metricsEngine.getEnqueuedJobCount(),
                Is.is(1L));

        Job nextJob = jobManager.nextJobForWorker(worker);
        byte[] result = {'r','e','s','u','l','t'};

        Assert.assertThat("Job pulled out is equal to job #1",
                nextJob.equals(jobOne),
                Is.is(true));

        // Complete the job
        jobManager.handleWorkCompletion(nextJob, result);

        verify(mockClientOne).sendWorkResults(jobOne.getJobHandle(), result);
        verify(mockClientTwo).sendWorkResults(jobOne.getJobHandle(), result);
    }

    // TODO: Verify that it will coalesce results if a job is submitted while a worker is working on the same one

    @Test
    public void sendsWorkDataResultsToClients() throws Exception {

        EngineClient mockClient = mock(EngineClient.class);

        Job jobOne = JobFactory.generateForegroundJob("reverseString");

        jobManager.storeJobForClient(jobOne, mockClient);

        Job nextJob = jobManager.nextJobForWorker(worker);
        byte[] data = {'r','e','s','u','l','t'};

        // Send back some data
        jobManager.handleWorkData(nextJob, data);

        verify(mockClient).sendWorkData(jobOne.getJobHandle(), data);
    }

}
