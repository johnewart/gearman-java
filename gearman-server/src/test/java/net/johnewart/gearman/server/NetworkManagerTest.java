package net.johnewart.gearman.server;

import io.netty.channel.Channel;
import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.interfaces.EngineClient;
import net.johnewart.gearman.common.interfaces.EngineWorker;
import net.johnewart.gearman.common.packets.request.SubmitJob;
import net.johnewart.gearman.common.packets.response.JobAssign;
import net.johnewart.gearman.common.packets.response.JobCreated;
import net.johnewart.gearman.common.packets.response.WorkCompleteResponse;
import net.johnewart.gearman.common.packets.response.WorkResponse;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.engine.core.JobManager;
import net.johnewart.gearman.server.net.NetworkEngineClient;
import net.johnewart.gearman.server.net.NetworkManager;
import net.johnewart.gearman.server.net.NetworkEngineWorker;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class NetworkManagerTest {
    private JobManager mockJobManager;
    private Channel mockClientChannel, mockWorkerChannel;

    private NetworkManager networkManager;
    private NetworkEngineClient client;
    private NetworkEngineWorker worker;

    public NetworkManagerTest()
    {
    }

    @Before
    public void initialize()
    {
        mockJobManager = Mockito.mock(JobManager.class);
        mockClientChannel = Mockito.mock(Channel.class);

        mockWorkerChannel = Mockito.mock(Channel.class);

        networkManager = new NetworkManager(mockJobManager);
        client = new NetworkEngineClient(mockClientChannel);
        worker = new NetworkEngineWorker(mockWorkerChannel);

    }

    @Test
    public void sendsJobCreatedPacketToClient() throws Exception
    {
        final String functionName = "bigJob";
        final String uniqueID = "rincewind";
        final byte[] submitData = {'b','a', 'r'};
        final Job job = new Job(functionName, uniqueID, submitData, JobPriority.NORMAL, false);
        SubmitJob submitJobPacket = new SubmitJob(functionName, uniqueID, submitData, false);

        Mockito.when(mockJobManager.storeJobForClient(Matchers.any(Job.class), Matchers.any(EngineClient.class))).thenReturn(job);

        networkManager.createJob(submitJobPacket, mockClientChannel);

        Mockito.verify(mockClientChannel).writeAndFlush(Matchers.any(JobCreated.class));

    }

    @Test
    public void sendWorkCompleteResponseToClientWhenFinished() throws Exception
    {
        final String functionName = "bigJob";
        final String uniqueID = "rincewind";
        final byte[] submitData = {'b','a', 'r'};
        final String[] jobHandle = new String[1];
        final Job job = new Job(functionName, uniqueID, submitData, JobPriority.NORMAL, false);

        // Pretend storing works A-OK!
        Mockito.when(mockJobManager.storeJobForClient(Matchers.any(Job.class), Matchers.any(EngineClient.class))).thenReturn(job);
        Mockito.when(mockJobManager.storeJob(Matchers.any(Job.class))).thenReturn(job);
        Mockito.when(mockJobManager.nextJobForWorker(Matchers.any(EngineWorker.class))).thenReturn(job);
        Mockito.when(mockJobManager.getCurrentJobForWorker(Matchers.any(EngineWorker.class))).thenReturn(job);

        // When we get a JOB_CREATED packet, pull out the job handle
        Mockito.when(mockClientChannel.writeAndFlush(Matchers.any(JobCreated.class))).thenAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                if (args[0].getClass().equals(JobCreated.class)) {
                    JobCreated response = (JobCreated) args[0];
                    jobHandle[0] = response.getJobHandle();
                }
                return null;
            }
        });

        // Submit a job as the client
        SubmitJob submitJobPacket = new SubmitJob(functionName, uniqueID, submitData, false);
        networkManager.createJob(submitJobPacket, mockClientChannel);
        Mockito.verify(mockClientChannel).writeAndFlush(Matchers.any(JobCreated.class));

        // Register our worker
        networkManager.registerAbility(functionName, mockWorkerChannel);

        // Fetch the job as our worker
        networkManager.nextJobForWorker(mockWorkerChannel, false);
        Mockito.verify(mockWorkerChannel).writeAndFlush(Matchers.any(JobAssign.class));

        byte[] resultdata = {'f','o', 'o'};
        WorkResponse workResponse = new WorkCompleteResponse(jobHandle[0], resultdata);
        networkManager.workResponse(workResponse, mockWorkerChannel);
        Mockito.verify(mockClientChannel).writeAndFlush(Matchers.any(WorkCompleteResponse.class));
    }

    @Test
    public void handleWorkerChannelDisconnected() throws Exception
    {
        final String functionName = "bigJob";
        final String uniqueID = "rincewind";
        final byte[] submitData = {'b','a', 'r'};
        final String[] jobHandle = new String[1];
        final Job job = new Job(functionName, uniqueID, submitData, JobPriority.NORMAL, false);

        // Pretend storing works A-OK!
        Mockito.when(mockJobManager.storeJobForClient(Matchers.any(Job.class), Matchers.any(EngineClient.class))).thenReturn(job);
        Mockito.when(mockJobManager.storeJob(Matchers.any(Job.class))).thenReturn(job);
        Mockito.when(mockJobManager.nextJobForWorker(Matchers.any(EngineWorker.class))).thenReturn(job);
        Mockito.when(mockJobManager.getCurrentJobForWorker(Matchers.any(EngineWorker.class))).thenReturn(job);

        // When we get a JOB_CREATED packet, pull out the job handle
        Mockito.when(mockClientChannel.writeAndFlush(Matchers.any(JobCreated.class))).thenAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                if (args[0].getClass().equals(JobCreated.class)) {
                    JobCreated response = (JobCreated) args[0];
                    jobHandle[0] = response.getJobHandle();
                }
                return null;
            }
        });

        // Submit a job as the client
        SubmitJob submitJobPacket = new SubmitJob(functionName, uniqueID, submitData, false);
        networkManager.createJob(submitJobPacket, mockClientChannel);
        Mockito.verify(mockClientChannel).writeAndFlush(Matchers.any(JobCreated.class));

        // Register our worker
        networkManager.registerAbility(functionName, mockWorkerChannel);

        Assert.assertThat("There is one worker connected",
                networkManager.getWorkerList().size(),
                Is.is(1));

        // Fetch the job as our worker
        networkManager.nextJobForWorker(mockWorkerChannel, false);

        // Simulate a disconnect
        networkManager.channelDisconnected(mockWorkerChannel);

        // Make sure it gets unregistered
        Mockito.verify(mockJobManager).unregisterWorker(Matchers.any(EngineWorker.class));

        Assert.assertThat("There are no workers connected",
                networkManager.getWorkerList().size(),
                Is.is(0));
    }

    @Test
    public void handleClientChannelDisconnected() throws Exception
    {
        final String functionName = "bigJob";
        final String uniqueID = "rincewind";
        final byte[] submitData = {'b','a', 'r'};
        final String[] jobHandle = new String[1];
        final Job job = new Job(functionName, uniqueID, submitData, JobPriority.NORMAL, false);

        // Pretend storing works A-OK!
        Mockito.when(mockJobManager.storeJobForClient(Matchers.any(Job.class), Matchers.any(EngineClient.class))).thenReturn(job);
        Mockito.when(mockJobManager.storeJob(Matchers.any(Job.class))).thenReturn(job);
        Mockito.when(mockJobManager.nextJobForWorker(Matchers.any(EngineWorker.class))).thenReturn(job);
        Mockito.when(mockJobManager.getCurrentJobForWorker(Matchers.any(EngineWorker.class))).thenReturn(job);

        // When we get a JOB_CREATED packet, pull out the job handle
        Mockito.when(mockClientChannel.writeAndFlush(Matchers.any(JobCreated.class))).thenAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                if (args[0].getClass().equals(JobCreated.class)) {
                    JobCreated response = (JobCreated) args[0];
                    jobHandle[0] = response.getJobHandle();
                }
                return null;
            }
        });

        // Submit a job as the client
        SubmitJob submitJobPacket = new SubmitJob(functionName, uniqueID, submitData, false);
        networkManager.createJob(submitJobPacket, mockClientChannel);
        Mockito.verify(mockClientChannel).writeAndFlush(Matchers.any(JobCreated.class));

        // Register our worker
        networkManager.registerAbility(functionName, mockWorkerChannel);

        // Fetch the job as our worker
        networkManager.nextJobForWorker(mockWorkerChannel, false);

        // TODO: Make sure to finish this ...
        Assert.assertThat("There is one client connected",
                networkManager.getClientList().size(),
                Is.is(1));

        networkManager.channelDisconnected(mockClientChannel);

        Assert.assertThat("There are no clients connected",
                networkManager.getClientList().size(),
                Is.is(0));

    }

    @Test
    public void handleWorkerSendsWorkData() throws Exception
    {
        final String functionName = "bigJob";
        final String uniqueID = "rincewind";
        final byte[] submitData = {'b','a', 'r'};
        final String[] jobHandle = new String[1];
        final Job job = new Job(functionName, uniqueID, submitData, JobPriority.NORMAL, false);

        // Worker sends a WORK_DATA request packet, that needs to be forwarded to the client(s)


        // Pretend storing works A-OK!
        Mockito.when(mockJobManager.storeJobForClient(Matchers.any(Job.class), Matchers.any(EngineClient.class))).thenReturn(job);
        Mockito.when(mockJobManager.storeJob(Matchers.any(Job.class))).thenReturn(job);
        Mockito.when(mockJobManager.nextJobForWorker(Matchers.any(EngineWorker.class))).thenReturn(job);
        Mockito.when(mockJobManager.getCurrentJobForWorker(Matchers.any(EngineWorker.class))).thenReturn(job);

        // When we get a JOB_CREATED packet, pull out the job handle
        Mockito.when(mockClientChannel.writeAndFlush(Matchers.any(JobCreated.class))).thenAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                if (args[0].getClass().equals(JobCreated.class)) {
                    JobCreated response = (JobCreated) args[0];
                    jobHandle[0] = response.getJobHandle();
                }
                return null;
            }
        });

        // Submit a job as the client
        SubmitJob submitJobPacket = new SubmitJob(functionName, uniqueID, submitData, false);
        networkManager.createJob(submitJobPacket, mockClientChannel);
        Mockito.verify(mockClientChannel).writeAndFlush(Matchers.any(JobCreated.class));

        // Register our worker
        networkManager.registerAbility(functionName, mockWorkerChannel);

        // Fetch the job as our worker
        networkManager.nextJobForWorker(mockWorkerChannel, false);

        // TODO: Make sure to finish this ...
        Assert.assertThat("There is one client connected",
                networkManager.getClientList().size(),
                Is.is(1));

        networkManager.channelDisconnected(mockClientChannel);

        Assert.assertThat("There are no clients connected",
                networkManager.getClientList().size(),
                Is.is(0));

    }



}
