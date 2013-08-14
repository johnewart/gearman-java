package org.gearman.server;

import io.netty.channel.Channel;
import io.netty.channel.Channel;
import org.gearman.common.Job;
import org.gearman.common.interfaces.Client;
import org.gearman.common.interfaces.Worker;
import org.gearman.common.packets.request.SubmitJob;
import org.gearman.common.packets.response.JobAssign;
import org.gearman.common.packets.response.JobCreated;
import org.gearman.common.packets.response.WorkCompleteResponse;
import org.gearman.common.packets.response.WorkResponse;
import org.gearman.constants.JobPriority;
import org.gearman.server.core.NetworkClient;
import org.gearman.server.core.NetworkWorker;
import org.gearman.server.net.NetworkManager;
import org.gearman.server.storage.JobManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class NetworkManagerTest {
    private JobManager mockJobManager;
    private Channel mockClientChannel, mockWorkerChannel;

    private NetworkManager networkManager;
    private NetworkClient client;
    private NetworkWorker worker;

    public NetworkManagerTest()
    {
    }

    @Before
    public void initialize()
    {
        mockJobManager = mock(JobManager.class);
        mockClientChannel = mock(Channel.class);

        mockWorkerChannel = mock(Channel.class);

        networkManager = new NetworkManager(mockJobManager);
        client = new NetworkClient(mockClientChannel);
        worker = new NetworkWorker(mockWorkerChannel);

    }

    @Test
    public void sendsJobCreatedPacketToClient() throws Exception
    {
        final String functionName = "bigJob";
        final String uniqueID = "rincewind";
        final byte[] submitData = {'b','a', 'r'};
        final Job job = new Job(functionName, uniqueID, submitData, JobPriority.NORMAL, false);
        SubmitJob submitJobPacket = new SubmitJob(functionName, uniqueID, submitData, false);

        when(mockJobManager.storeJobForClient(any(Job.class), any(Client.class))).thenReturn(job);

        networkManager.createJob(submitJobPacket, mockClientChannel);

        verify(mockClientChannel).writeAndFlush(any(JobCreated.class));

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
        when(mockJobManager.storeJobForClient(any(Job.class), any(Client.class))).thenReturn(job);
        when(mockJobManager.storeJob(any(Job.class))).thenReturn(job);
        when(mockJobManager.nextJobForWorker(any(Worker.class))).thenReturn(job);
        when(mockJobManager.getCurrentJobForWorker(any(Worker.class))).thenReturn(job);

        // When we get a JOB_CREATED packet, pull out the job handle
        when(mockClientChannel.writeAndFlush(any(JobCreated.class))).thenAnswer(new Answer() {
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
        verify(mockClientChannel).writeAndFlush(any(JobCreated.class));

        // Register our worker
        networkManager.registerAbility(functionName, mockWorkerChannel);

        // Fetch the job as our worker
        networkManager.nextJobForWorker(mockWorkerChannel, false);
        verify(mockWorkerChannel).writeAndFlush(any(JobAssign.class));

        byte[] resultdata = {'f','o', 'o'};
        WorkResponse workResponse = new WorkCompleteResponse(jobHandle[0], resultdata);
        networkManager.workResponse(workResponse, mockWorkerChannel);
        verify(mockClientChannel).writeAndFlush(any(WorkCompleteResponse.class));
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
        when(mockJobManager.storeJobForClient(any(Job.class), any(Client.class))).thenReturn(job);
        when(mockJobManager.storeJob(any(Job.class))).thenReturn(job);
        when(mockJobManager.nextJobForWorker(any(Worker.class))).thenReturn(job);
        when(mockJobManager.getCurrentJobForWorker(any(Worker.class))).thenReturn(job);

        // When we get a JOB_CREATED packet, pull out the job handle
        when(mockClientChannel.writeAndFlush(any(JobCreated.class))).thenAnswer(new Answer() {
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
        verify(mockClientChannel).writeAndFlush(any(JobCreated.class));

        // Register our worker
        networkManager.registerAbility(functionName, mockWorkerChannel);

        assertThat("There is one worker connected",
                networkManager.getWorkerList().size(),
                is(1));

        // Fetch the job as our worker
        networkManager.nextJobForWorker(mockWorkerChannel, false);

        // Simulate a disconnect
        networkManager.channelDisconnected(mockWorkerChannel);

        // Make sure it gets unregistered
        verify(mockJobManager).unregisterWorker(any(Worker.class));

        assertThat("There are no workers connected",
                networkManager.getWorkerList().size(),
                is(0));
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
        when(mockJobManager.storeJobForClient(any(Job.class), any(Client.class))).thenReturn(job);
        when(mockJobManager.storeJob(any(Job.class))).thenReturn(job);
        when(mockJobManager.nextJobForWorker(any(Worker.class))).thenReturn(job);
        when(mockJobManager.getCurrentJobForWorker(any(Worker.class))).thenReturn(job);

        // When we get a JOB_CREATED packet, pull out the job handle
        when(mockClientChannel.writeAndFlush(any(JobCreated.class))).thenAnswer(new Answer() {
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
        verify(mockClientChannel).writeAndFlush(any(JobCreated.class));

        // Register our worker
        networkManager.registerAbility(functionName, mockWorkerChannel);

        // Fetch the job as our worker
        networkManager.nextJobForWorker(mockWorkerChannel, false);

        // TODO: Make sure to finish this ...
        assertThat("There is one client connected",
                networkManager.getClientList().size(),
                is(1));

        networkManager.channelDisconnected(mockClientChannel);

        assertThat("There are no clients connected",
                networkManager.getClientList().size(),
                is(0));

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
        when(mockJobManager.storeJobForClient(any(Job.class), any(Client.class))).thenReturn(job);
        when(mockJobManager.storeJob(any(Job.class))).thenReturn(job);
        when(mockJobManager.nextJobForWorker(any(Worker.class))).thenReturn(job);
        when(mockJobManager.getCurrentJobForWorker(any(Worker.class))).thenReturn(job);

        // When we get a JOB_CREATED packet, pull out the job handle
        when(mockClientChannel.writeAndFlush(any(JobCreated.class))).thenAnswer(new Answer() {
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
        verify(mockClientChannel).writeAndFlush(any(JobCreated.class));

        // Register our worker
        networkManager.registerAbility(functionName, mockWorkerChannel);

        // Fetch the job as our worker
        networkManager.nextJobForWorker(mockWorkerChannel, false);

        // TODO: Make sure to finish this ...
        assertThat("There is one client connected",
                networkManager.getClientList().size(),
                is(1));

        networkManager.channelDisconnected(mockClientChannel);

        assertThat("There are no clients connected",
                networkManager.getClientList().size(),
                is(0));

    }



}
