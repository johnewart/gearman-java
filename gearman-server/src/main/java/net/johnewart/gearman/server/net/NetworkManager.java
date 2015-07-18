package net.johnewart.gearman.server.net;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableList;
import io.netty.channel.Channel;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.JobStatus;
import net.johnewart.gearman.common.interfaces.EngineClient;
import net.johnewart.gearman.common.packets.Packet;
import net.johnewart.gearman.common.packets.request.EchoRequest;
import net.johnewart.gearman.common.packets.request.GetStatus;
import net.johnewart.gearman.common.packets.request.OptionRequest;
import net.johnewart.gearman.common.packets.request.SubmitJob;
import net.johnewart.gearman.common.packets.response.*;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.constants.PacketType;
import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.engine.exceptions.IllegalJobStateTransitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;


/**
 * Serves as an interface between the packet handler and the job manager
 * General flow:
 *   Netty - CODEC - PacketHandler - NetworkManager - JobManager
 *
 */
public class NetworkManager {
    private final JobManager jobManager;
    private final ConcurrentHashMap<Channel, NetworkEngineWorker> workers;
    private final ConcurrentHashMap<Channel, NetworkEngineClient> clients;
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
            NetworkEngineWorker worker = workers.get(channel);
            jobManager.unregisterWorker(worker);
            workers.remove(channel);
        } else if (clients.containsKey(channel)) {
            EngineClient client = clients.get(channel);
            jobManager.unregisterClient(client);
            clients.remove(channel);
        }
    }

    public void sleepingWorker(Channel channel)
    {
        // Remove from any worker lists
        if(workers.containsKey(channel))
        {
            NetworkEngineWorker worker = workers.get(channel);
            jobManager.markWorkerAsAsleep(worker);
        }
    }

    public void registerAbility(String functionName, Channel channel) {
        NetworkEngineWorker worker = findOrCreateWorker(channel);
        worker.addAbility(functionName);
        jobManager.registerWorkerAbility(functionName, worker);
    }

    public void unregisterAbility(String functionName, Channel channel) {
        if(workers.containsKey(channel))
        {
            NetworkEngineWorker worker = workers.get(channel);
            worker.removeAbility(functionName);
            jobManager.unregisterWorkerAbility(functionName, worker);
        }
    }

    public void nextJobForWorker(Channel channel, boolean uniqueID) {
        if(workers.containsKey(channel))
        {
            NetworkEngineWorker worker = workers.get(channel);
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
        EngineClient client = findOrCreateClient(channel);
        String funcName = packet.getFunctionName();
        String uniqueID = packet.getUniqueId();
        byte[] data = packet.getData();
        JobPriority priority = packet.getPriority();
        boolean isBackground = packet.isBackground();
        long timeToRun = -1;

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
        EngineClient client = findOrCreateClient(channel);
        JobStatus jobStatus =  jobManager.checkJobStatus(getStatus.jobHandle.get());

        StatusRes result = new StatusRes(jobStatus);
        client.send(result);
    }

    public void updateJobStatus(WorkStatus workStatus) {
        String jobHandle = workStatus.getJobHandle();
        int completeNumerator = workStatus.getCompleteNumerator();
        int completeDenominator = workStatus.getCompleteDenominator();
        jobManager.updateJobStatus(jobHandle, completeNumerator, completeDenominator);
    }

    private EngineClient findOrCreateClient(Channel channel)
    {
        NetworkEngineClient client;

        if(clients.containsKey(channel))
        {
            client = clients.get(channel);
        } else {
            client = new NetworkEngineClient(channel);
            clients.put(channel, client);
        }

        return client;
    }

    private NetworkEngineWorker findOrCreateWorker(Channel channel)
    {
        NetworkEngineWorker worker;

        if(workers.containsKey(channel))
        {
            worker = workers.get(channel);
        } else {
            worker = new NetworkEngineWorker(channel);
            workers.put(channel, worker);
        }

        return worker;
    }

    public void workResponse(WorkResponse response, Channel channel) {
        if(workers.containsKey(channel))
        {
            NetworkEngineWorker worker = workers.get(channel);
            Job currentJob = jobManager.getJobByJobHandle(response.getJobHandle());

            if (currentJob != null) {
                switch(response.getType())
                {
                    case WORK_COMPLETE:
                        jobManager.handleWorkCompletion(currentJob, ((WorkCompleteResponse) response).getData());
                        break;

                    case WORK_DATA:
                        jobManager.handleWorkData(currentJob, ((WorkDataResponse) response).getData());
                        break;

                    case WORK_EXCEPTION:
                        jobManager.handleWorkException(currentJob, ((WorkExceptionResponse) response).getException());
                        break;

                    case WORK_WARNING:
                        jobManager.handleWorkWarning(currentJob, ((WorkWarningResponse) response).getData());
                        break;

                    case WORK_FAIL:
                        jobManager.handleWorkFailure(currentJob);
                        break;

                    default:
                        break;
                }
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
        boolean isRunning = job.isRunning();
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


    public ImmutableList<NetworkEngineClient> getClientList()
    {
        return ImmutableList.copyOf(clients.values());
    }

    public ImmutableList<NetworkEngineWorker> getWorkerList()
    {
        return ImmutableList.copyOf(workers.values());
    }

    public void handleEchoRequest(EchoRequest request, Channel channel) {
        EchoResponse response = new EchoResponse(request);
        channel.write(response);
    }

    public void resetWorkerAbilities(Channel channel) {
        NetworkEngineWorker worker = findOrCreateWorker(channel);
        jobManager.resetWorkerAbilities(worker);
    }

    public void handleOptionRequest(OptionRequest packet, Channel channel) {
        //TODO: mark that the client wants exceptions (why would it not want them?)
        OptionResponse response = new OptionResponse(packet.getOption());
        channel.write(response);
    }
}
