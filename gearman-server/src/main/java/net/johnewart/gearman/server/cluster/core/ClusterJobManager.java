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
import net.johnewart.gearman.engine.queue.factories.JobQueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterJobManager extends JobManager implements MessageListener<WorkMessage> {
    private static Logger LOG = LoggerFactory.getLogger(ClusterJobManager.class);

    private final HazelcastInstance hazelcast;
    private final ITopic<WorkMessage> workMessageTopic;
    private final static String WORK_TOPIC = "work";

    public ClusterJobManager(JobQueueFactory jobQueueFactory,
                             JobHandleFactory jobHandleFactory,
                             UniqueIdFactory uniqueIdFactory,
                             HazelcastInstance hazelcast) {
        super(jobQueueFactory, jobHandleFactory, uniqueIdFactory);
        this.hazelcast = hazelcast;
        workMessageTopic = this.hazelcast.getTopic (WORK_TOPIC);
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
        Job storedJob = super.storeJobForClient(job, client);
        WorkMessage newJobMessage =
                new WorkMessage(storedJob.getUniqueID(), storedJob.getFunctionName(), PacketType.SUBMIT_JOB, new byte[0]);
        workMessageTopic.publish(newJobMessage);
        return storedJob;
    }

    @Override
    public void onMessage(Message<WorkMessage> message) {
        final WorkMessage workMessage = message.getMessageObject();

        switch (workMessage.type) {
            case WORK_COMPLETE:
                LOG.debug("Work " + workMessage.uniqueId + " completed in " + workMessage.functionName);
                Job currentJob = getJobQueue(workMessage.functionName).findJobByUniqueId(workMessage.uniqueId);
                if (currentJob != null) {
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
                Job dataJob = getJobQueue(workMessage.functionName).findJobByUniqueId(workMessage.uniqueId);
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
