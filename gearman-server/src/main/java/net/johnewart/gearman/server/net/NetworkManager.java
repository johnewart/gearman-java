package net.johnewart.gearman.server.net;

import com.google.common.collect.ImmutableList;
import com.yammer.metrics.annotation.Metered;
import com.yammer.metrics.annotation.Timed;
import io.netty.channel.Channel;
import net.johnewart.gearman.common.JobState;
import net.johnewart.gearman.common.interfaces.Client;
import net.johnewart.gearman.common.packets.response.EchoResponse;
import net.johnewart.gearman.common.packets.response.JobAssign;
import net.johnewart.gearman.common.packets.response.JobAssignUniq;
import net.johnewart.gearman.common.packets.response.JobCreated;
import net.johnewart.gearman.common.packets.response.NoJob;
import net.johnewart.gearman.common.packets.response.StatusRes;
import net.johnewart.gearman.common.packets.response.WorkCompleteResponse;
import net.johnewart.gearman.common.packets.response.WorkDataResponse;
import net.johnewart.gearman.common.packets.response.WorkExceptionResponse;
import net.johnewart.gearman.common.packets.response.WorkResponse;
import net.johnewart.gearman.common.packets.response.WorkStatus;
import net.johnewart.gearman.common.packets.response.WorkWarningResponse;
import net.johnewart.gearman.server.core.NetworkClient;
import net.johnewart.gearman.server.core.NetworkWorker;
import net.johnewart.gearman.server.storage.JobManager;
import net.johnewart.gearman.common.JobStatus;
import net.johnewart.gearman.common.interfaces.Worker;
import net.johnewart.gearman.common.packets.Packet;
import net.johnewart.gearman.common.packets.request.EchoRequest;
import net.johnewart.gearman.common.packets.request.GetStatus;
import net.johnewart.gearman.common.packets.request.SubmitJob;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.constants.PacketType;
import net.johnewart.gearman.server.exceptions.IllegalJobStateTransitionException;
import net.johnewart.gearman.common.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;


/**
 * Serves as an interface between the packet handler and the job manager
 * General flow:
 *   Netty -> CODEC -> PacketHandler -> NetworkManager -> JobManager
 *
 */
public class NetworkManager {
    private final JobManager jobManager;
    private final ConcurrentHashMap<Channel, NetworkWorker> workers;
    private final ConcurrentHashMap<Channel, NetworkClient> clients;
    private static Logger LOG = LoggerFactory.getLogger(NetworkManager.class);

    public NetworkManager(JobManager jobManager)
    {
        this.jobManager = jobManager;
        this.workers = new ConcurrentHashMap<>();
        this.clients = new ConcurrentHashMap<>();
    }

    public synchronized void channelDisconnected(Channel channel)
    {
        if(workers.containsKey(channel))
        {
            Worker worker = workers.get(channel);
            jobManager.unregisterWorker(worker);
            workers.remove(channel);
        } else if (clients.containsKey(channel)) {
            Client client = clients.get(channel);
            jobManager.unregisterClient(client);
            clients.remove(channel);
        }
    }

    public void sleepingWorker(Channel channel)
    {
        // Remove from any worker lists
        if(workers.containsKey(channel))
        {
            Worker worker = workers.get(channel);
            jobManager.sleepingWorker(worker);
        }
    }

    public void registerAbility(String functionName, Channel channel) {
        Worker worker = findOrCreateWorker(channel);
        worker.addAbility(functionName);
        jobManager.registerWorkerAbility(functionName, worker);
    }

    public void unregisterAbility(String functionName, Channel channel) {
        if(workers.containsKey(channel))
        {
            Worker worker = workers.get(channel);
            jobManager.unregisterWorkerAbility(functionName, worker);
        }
    }

    public void nextJobForWorker(Channel channel, boolean uniqueID) {
        if(workers.containsKey(channel))
        {
            NetworkWorker worker = workers.get(channel);
            Job nextJob = jobManager.nextJobForWorker(worker);

            if(nextJob != null)
            {
                Packet packet;

                if(uniqueID)
                {
                    packet = createJobAssignUniqPacket(nextJob);
                } else {
                    packet = createJobAssignPacket(nextJob);
                }

                try {
                    worker.send(packet);
                } catch (Exception e) {
                    LOG.error("Unable to write to worker. Re-enqueing job.");
                    try {
                        jobManager.reEnqueueJob(nextJob);
                    } catch (IllegalJobStateTransitionException ee) {
                        LOG.error("Error re-enqueing after failed transmission: ", ee);
                    }
                }
            } else {
                worker.send(new NoJob());
            }
        }
    }


    @Timed
    @Metered
    public void createJob(SubmitJob packet, Channel channel) {
        Client client = findOrCreateClient(channel);
        String funcName = packet.getFunctionName();
        String uniqueID = packet.getUniqueId();
        byte[] data = packet.getData();
        JobPriority priority = packet.getPriority();
        boolean isBackground = packet.isBackground();
        long timeToRun = -1;

        if(packet.getUniqueId().isEmpty())
            uniqueID = jobManager.generateUniqueID(funcName);

        if(packet.getType() == PacketType.SUBMIT_JOB_EPOCH)
        {
            timeToRun = packet.getEpoch();
        }

        // This could return an existing job, or the newly generated one
        Job storedJob = jobManager.storeJobForClient(new Job(funcName, uniqueID, data, priority, isBackground, timeToRun), client);

        if(storedJob != null)
        {
            client.setCurrentJob(storedJob);
            client.send(createJobCreatedPacket(storedJob));
        } else {
            // TODO: send a failure/error packet?
        }
    }

    public void checkJobStatus(GetStatus getStatus, Channel channel) {
        Client client = findOrCreateClient(channel);
        JobStatus jobStatus =  jobManager.checkJobStatus(getStatus.jobHandle.get());

        StatusRes result = new StatusRes(jobStatus);
        client.send(result);
    }

    public void updateJobStatus(WorkStatus workStatus) {
        String jobHandle = workStatus.getJobHandle();
        int completeNumerator = workStatus.getCompletenumerator();
        int completeDenominator = workStatus.getCompletedenominator();
        jobManager.updateJobStatus(jobHandle, completeNumerator, completeDenominator);
    }

    private Client findOrCreateClient(Channel channel)
    {
        NetworkClient client;

        if(clients.containsKey(channel))
        {
            client = clients.get(channel);
        } else {
            client = new NetworkClient(channel);
            clients.put(channel, client);
        }

        return client;
    }

    private Worker findOrCreateWorker(Channel channel)
    {
        NetworkWorker worker;

        if(workers.containsKey(channel))
        {
            worker = workers.get(channel);
        } else {
            worker = new NetworkWorker(channel);
            workers.put(channel, worker);
        }

        return worker;
    }

    public void workResponse(WorkResponse packet, Channel channel) {
        if(workers.containsKey(channel))
        {
            Worker worker = workers.get(channel);
            Job currentJob = jobManager.getCurrentJobForWorker(worker);

            switch(packet.getType())
            {
                case WORK_COMPLETE:
                    jobManager.workComplete(currentJob,((WorkCompleteResponse)packet).getData());
                    break;

                case WORK_DATA:
                    jobManager.workData(currentJob, ((WorkDataResponse)packet).getData());
                    break;

                case WORK_EXCEPTION:
                    jobManager.workException(currentJob, ((WorkExceptionResponse)packet).getException());
                    break;

                case WORK_WARNING:
                    jobManager.workWarning(currentJob, ((WorkWarningResponse)packet).getData());
                    break;

                case WORK_FAIL:
                    jobManager.workFail(currentJob);
                    break;

                default:
                    break;
            }
        }
    }

    public JobManager getJobManager() {
        return jobManager;
    }

    public final Packet createJobAssignPacket(Job job) {
        return  new JobAssign(job.getJobHandle(), job.getFunctionName(), job.getData());
    }

    public final Packet createJobAssignUniqPacket(Job job) {
        return new JobAssignUniq(job.getJobHandle(), job.getFunctionName(), job.getUniqueID(), job.getData());
    }
    public final Packet createJobCreatedPacket(Job job) {
        return new JobCreated(job.getJobHandle());
    }

    public final Packet createWorkStatusPacket(Job job) {
        return new WorkStatus(job.getJobHandle(), job.getNumerator(), job.getDenominator());
    }

    public final Packet createStatusResponsePacket(Job job) {
        boolean isRunning = job.getState() == JobState.WORKING;
        boolean knownState = true;
        int numerator = job.getNumerator();
        int denominator = job.getDenominator();
        String jobHandle = job.getJobHandle();

        if(numerator == 0 && denominator == 0)
        {
            knownState = false;
        }

        return new StatusRes(jobHandle, isRunning, knownState, numerator, denominator);
    }


    public ImmutableList<NetworkClient> getClientList()
    {
        return ImmutableList.copyOf(clients.values());
    }

    public ImmutableList<NetworkWorker> getWorkerList()
    {
        return ImmutableList.copyOf(workers.values());
    }

    public void handleEchoRequest(EchoRequest request, Channel channel) {
        EchoResponse response = new EchoResponse(request);
        channel.write(response);
    }
}
