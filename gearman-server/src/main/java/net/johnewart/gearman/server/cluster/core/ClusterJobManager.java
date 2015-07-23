package net.johnewart.gearman.server.cluster.core;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.interfaces.EngineClient;
import net.johnewart.gearman.common.interfaces.JobHandleFactory;
import net.johnewart.gearman.constants.PacketType;
import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.engine.core.UniqueIdFactory;
import net.johnewart.gearman.engine.exceptions.EnqueueException;
import net.johnewart.gearman.engine.metrics.QueueMetrics;
import net.johnewart.gearman.engine.queue.JobQueue;
import net.johnewart.gearman.engine.queue.factories.JobQueueFactory;
import net.johnewart.gearman.engine.storage.NoopExceptionStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterJobManager extends JobManager implements MessageListener<WorkMessage> {
    private static Logger LOG = LoggerFactory.getLogger(ClusterJobManager.class);

    private final ITopic<WorkMessage> workMessageTopic;
    private final static String WORK_TOPIC = "work";

    public ClusterJobManager(JobQueueFactory jobQueueFactory,
                             JobHandleFactory jobHandleFactory,
                             UniqueIdFactory uniqueIdFactory,
                             HazelcastInstance hazelcast,
                             QueueMetrics queueMetrics) {
        super(jobQueueFactory, jobHandleFactory, uniqueIdFactory, new NoopExceptionStorageEngine(), queueMetrics);
        workMessageTopic = hazelcast.getTopic(WORK_TOPIC);
        workMessageTopic.addMessageListener(this);
    }

    @Override
    public synchronized void handleWorkCompletion(Job job, byte[] data)
    {
        LOG.info("Work Complete!");
        super.handleWorkCompletion(job, data);
        WorkMessage workCompleteMessage =
                new WorkMessage(job.getUniqueID(), job.getFunctionName(), PacketType.WORK_COMPLETE, data);
        workMessageTopic.publish(workCompleteMessage);
    }

    @Override
    public synchronized Job storeJobForClient(Job job, EngineClient client) {
        try
        {
            Job storedJob = super.storeJobForClient(job, client);
            WorkMessage newJobMessage =
                    new WorkMessage(storedJob.getUniqueID(), storedJob.getFunctionName(), PacketType.SUBMIT_JOB, new byte[0]);
            workMessageTopic.publish(newJobMessage);
            return storedJob;
        }
        catch (EnqueueException e)
        {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void onMessage(Message<WorkMessage> message) {
        final WorkMessage workMessage = message.getMessageObject();
        final JobQueue queue = getJobQueue(workMessage.functionName);

        // Nothing to do, we don't know about that queue
        // TODO: If this happens, we're likely split-brain, handle this better?
        if (queue == null) {
            LOG.error("Queue (" + workMessage.functionName + ") for work-message doesn't exist!");
            return;
        }

        switch (workMessage.type) {
            case WORK_COMPLETE:
                LOG.debug("Work " + workMessage.uniqueId + " completed in " + workMessage.functionName);
                Job currentJob = queue.findJobByUniqueId(workMessage.uniqueId);
                if (currentJob != null)
                {
                    LOG.debug("Notifying any pending clients that  " + workMessage.uniqueId + " is complete.");
                    super.notifyClientsOfCompletion(currentJob, workMessage.data);
                }
                break;
            case SUBMIT_JOB:
                // Wake up any relevant workers
                LOG.debug("Job submitted for " + workMessage.functionName + ".");
                getWorkerPool(workMessage.functionName)
                        .wakeupWorkers();
                break;
            case WORK_DATA:
                LOG.debug("Data for " + workMessage.uniqueId + " in  " + workMessage.functionName);
                Job dataJob = queue.findJobByUniqueId(workMessage.uniqueId);
                if (dataJob != null) {
                    LOG.debug("Notifying any pending clients of updated data  " + workMessage.uniqueId + " is complete.");
                    super.notifyClientsOfCompletion(dataJob, workMessage.data);
                }
                break;
            default:
                LOG.warn("Unhandled work message");
        }
    }
}
