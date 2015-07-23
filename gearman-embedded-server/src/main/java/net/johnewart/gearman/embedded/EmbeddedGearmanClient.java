package net.johnewart.gearman.embedded;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.JobStatus;
import net.johnewart.gearman.common.client.AbstractGearmanClient;
import net.johnewart.gearman.common.events.GearmanClientEventListener;
import net.johnewart.gearman.common.interfaces.EngineClient;
import net.johnewart.gearman.common.packets.Packet;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.engine.exceptions.EnqueueException;
import net.johnewart.gearman.exceptions.JobSubmissionException;
import net.johnewart.gearman.exceptions.WorkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

public class EmbeddedGearmanClient extends AbstractGearmanClient implements EngineClient {
    private final Logger LOG = LoggerFactory.getLogger(EmbeddedGearmanClient.class);
    private final EmbeddedGearmanServer server;
    private final Object activeJobLock;
    private final HashMap<String, byte[]> results;

    public EmbeddedGearmanClient(EmbeddedGearmanServer server) {
        this.server = server;
        this.activeJobLock = new Object();
        this.results = new HashMap<>();
    }

    @Override
    public String submitFutureJob(String callback, byte[] data, Date whenToRun) throws JobSubmissionException {
        final long timeToRun = whenToRun.getTime() / 1000;
        final String uniqueId = UUID.randomUUID().toString();
        Job submitted = doJobSubmission(callback, uniqueId, data, JobPriority.NORMAL, true, timeToRun);
        return submitted.getJobHandle();
    }

    @Override
    public String submitJobInBackground(String callback, byte[] data) throws JobSubmissionException {
        return submitJobInBackground(callback, data, JobPriority.NORMAL);
    }

    @Override
    public String submitJobInBackground(String callback, byte[] data, JobPriority priority) throws JobSubmissionException {
        final String uniqueId = UUID.randomUUID().toString();
        Job submitted = doJobSubmission(callback, uniqueId, data, priority, true, 0L);
        return submitted.getJobHandle();
    }

    @Override
    public byte[] submitJob(String callback, byte[] data) throws JobSubmissionException {

        Job job = new Job.Builder()
                .data(data)
                .functionName(callback)
                .build();

        try
        {
            server.submitJob(job, this);

            synchronized (activeJobLock)
            {
                try
                {
                    activeJobLock.wait();
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                    return null;
                }
            }
        } catch (EnqueueException e) {
            throw new JobSubmissionException();
        }

        return results.get(job.getJobHandle());
    }

    @Override
    public byte[] submitJob(String callback, byte[] data, JobPriority priority) throws JobSubmissionException, WorkException {
        return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public JobStatus getStatus(String jobHandle) {
        // NO-OP
        return null;
    }

    @Override
    public void registerEventListener(GearmanClientEventListener listener) {
        // NO-OP
    }

    @Override
    public void shutdown() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Job getCurrentJob() {
        // NO-OP
        return null;
    }

    @Override
    public void setCurrentJob(Job job) {
        // NO-OP
    }

    @Override
    public void sendWorkResults(String jobHandle, byte[] data) {
        synchronized (activeJobLock) {
            activeJobLock.notify();
        }
        results.put(jobHandle, data);
    }

    @Override
    public void sendWorkData(String jobHandle, byte[] data) {
        synchronized (activeJobLock) {
            activeJobLock.notify();
        }
        results.put(jobHandle, data);
    }

    @Override
    public void sendWorkException(String jobHandle, byte[] exception) {

    }

    @Override
    public void sendWorkFail(String jobHandle) {
        // NO-OP
    }

    @Override
    public void sendWorkWarning(String jobHandle, byte[] warning) {
        // NO-OP
    }

    @Override
    public void sendWorkStatus(JobStatus jobStatus) {
        // NO-OP
    }

    @Override
    public void send(Packet packet) {
        // NO-OP
    }

    private Job doJobSubmission(String callback,
                                   String uniqueID,
                                   byte[] data,
                                   JobPriority priority,
                                   boolean isBackground,
                                   long timeToRun) throws JobSubmissionException {
        try
        {
            final Job requestedJob = new Job(callback, uniqueID, data, priority, isBackground, timeToRun);
            return server.submitJob(requestedJob, this);
        } catch (EnqueueException e)
        {
            throw new JobSubmissionException();
        }
    }


}
