package org.gearman.server.net;

import com.yammer.metrics.annotation.Metered;
import com.yammer.metrics.annotation.Timed;
import org.gearman.common.JobState;
import org.gearman.common.JobStatus;
import org.gearman.common.interfaces.Client;
import org.gearman.common.interfaces.Worker;
import org.gearman.common.packets.Packet;
import org.gearman.common.packets.request.GetStatus;
import org.gearman.common.packets.request.SubmitJob;
import org.gearman.common.packets.response.*;
import org.gearman.constants.JobPriority;
import org.gearman.constants.PacketType;
import org.gearman.server.exceptions.IllegalJobStateTransitionException;
import org.gearman.common.Job;
import org.gearman.server.storage.JobStore;
import org.gearman.server.core.*;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NetworkManager {
    private final JobStore jobStore;
    private final ConcurrentHashMap<Channel, NetworkWorker> workers;
    private final ConcurrentHashMap<Channel, NetworkClient> clients;
    private static Logger LOG = LoggerFactory.getLogger(NetworkManager.class);

    public NetworkManager(JobStore jobStore)
    {
        this.jobStore = jobStore;
        this.workers = new ConcurrentHashMap<>();
        this.clients = new ConcurrentHashMap<>();
    }

    public synchronized void channelDisconnected(Channel channel)
    {
        if(workers.containsKey(channel))
        {
            Worker worker = workers.get(channel);
            jobStore.unregisterWorker(worker);
            workers.remove(channel);
        } else if (clients.containsKey(channel)) {
            Client client = clients.get(channel);
            jobStore.unregisterClient(client);
            clients.remove(channel);
        }
    }

    public void sleepingWorker(Channel channel)
    {
        // Remove from any worker lists
        if(workers.containsKey(channel))
        {
            Worker worker = workers.get(channel);
            jobStore.sleepingWorker(worker);
        }
    }

    public void registerAbility(String functionName, Channel channel) {
        Worker worker = findOrCreateWorker(channel);
        worker.addAbility(functionName);
        jobStore.registerWorkerAbility(functionName, worker);
    }

    public void unregisterAbility(String functionName, Channel channel) {
        if(workers.containsKey(channel))
        {
            Worker worker = workers.get(channel);
            jobStore.unregisterWorkerAbility(functionName, worker);
        }
    }

    public void nextJobForWorker(Channel channel, boolean uniqueID) {
        if(workers.containsKey(channel))
        {
            NetworkWorker worker = workers.get(channel);
            Job nextJob = jobStore.nextJobForWorker(worker);

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
                        jobStore.reEnqueueJob(nextJob);
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
            uniqueID = jobStore.generateUniqueID(funcName);

        if(packet.getType() == PacketType.SUBMIT_JOB_EPOCH)
        {
            timeToRun = packet.getEpoch();
        }

        // This could return an existing job, or the newly generated one
        Job job = jobStore.createAndStoreJob(funcName, uniqueID, data, priority, isBackground, timeToRun);

        if(job != null)
        {
            if(!job.isBackground())
                job.addClient(client);
            client.setCurrentJob(job);
            client.send(createJobCreatedPacket(job));
        } else {
            // TODO: send a failure/error packet?
        }
    }

    public void checkJobStatus(GetStatus getStatus, Channel channel) {
        Client client = findOrCreateClient(channel);
        JobStatus jobStatus =  jobStore.checkJobStatus(getStatus.jobHandle.get());

        StatusRes result = new StatusRes(jobStatus);
        client.send(result);
    }

    public void updateJobStatus(WorkStatus workStatus) {
        String jobHandle = workStatus.jobHandle.get();
        int completeNumerator = workStatus.completenumerator;
        int completeDenominator = workStatus.completedenominator;
        jobStore.updateJobStatus(jobHandle, completeNumerator, completeDenominator);
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

    public void workComplete(WorkResponse packet, Channel channel) {
        if(workers.containsKey(channel))
        {
            Worker worker = workers.get(channel);
            Job currentJob = jobStore.getCurrentJobForWorker(worker);

            if(currentJob != null)
            {
                Set<Client> clients = currentJob.getClients();

                for(Client client : clients) {
                    client.send(packet);
                }
            }

            jobStore.workComplete(currentJob, worker);
        }
    }

    public JobStore getJobStore() {
        return jobStore;
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
}
