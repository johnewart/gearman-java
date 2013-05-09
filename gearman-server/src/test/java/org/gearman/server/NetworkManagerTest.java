package org.gearman.server;

import org.gearman.common.Job;
import org.gearman.common.interfaces.Worker;
import org.gearman.common.packets.request.SubmitJob;
import org.gearman.common.packets.response.JobAssign;
import org.gearman.common.packets.response.JobCreated;
import org.gearman.common.packets.response.WorkComplete;
import org.gearman.common.packets.response.WorkResponse;
import org.gearman.constants.JobPriority;
import org.gearman.server.core.NetworkClient;
import org.gearman.server.core.NetworkWorker;
import org.gearman.server.net.NetworkManager;
import org.gearman.server.storage.JobStore;
import org.jboss.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class NetworkManagerTest {
    private final JobStore mockJobStore;
    private final NetworkManager networkManager;
    private final Channel mockClientChannel, mockWorkerChannel;
    private final NetworkClient client;
    private final NetworkWorker worker;

    public NetworkManagerTest()
    {
        mockJobStore = mock(JobStore.class);
        networkManager = new NetworkManager(mockJobStore);
        mockClientChannel = mock(Channel.class);
        mockWorkerChannel = mock(Channel.class);

        client = new NetworkClient(mockClientChannel);
        worker = new NetworkWorker(mockWorkerChannel);
    }

    @Before
    public void initialize()
    {

    }

    @Test
    public void sendsJobCreatedPacketToClient() throws Exception
    {
        final String functionName = "bigJob";
        final String uniqueID = "rincewind";
        final byte[] submitData = {'b','a', 'r'};
        final Job job = new Job(functionName, uniqueID, submitData, JobPriority.NORMAL, false);
        SubmitJob submitJobPacket = new SubmitJob(functionName, uniqueID, submitData, false);

        when(mockJobStore.createAndStoreJob(anyString(), anyString(), any(byte[].class),
                any(JobPriority.class), anyBoolean(), anyLong())).thenReturn(job);

        networkManager.createJob(submitJobPacket, mockClientChannel);

        verify(mockClientChannel).write(any(JobCreated.class));

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
        when(mockJobStore.createAndStoreJob(
                    anyString(), anyString(), any(byte[].class),
                    any(JobPriority.class), anyBoolean(), anyLong()
                )).thenReturn(job);
        when(mockJobStore.storeJob(any(Job.class))).thenReturn(true);
        when(mockJobStore.nextJobForWorker(any(Worker.class))).thenReturn(job);
        when(mockJobStore.getCurrentJobForWorker(any(Worker.class))).thenReturn(job);

        // When we get a JOB_CREATED packet, pull out the job handle
        when(mockClientChannel.write(any(JobCreated.class))).thenAnswer(new Answer() {
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
        verify(mockClientChannel).write(any(JobCreated.class));

        // Register our worker
        networkManager.registerAbility(functionName, mockWorkerChannel);

        // Fetch the job as our worker
        networkManager.nextJobForWorker(mockWorkerChannel, false);
        verify(mockWorkerChannel).write(any(JobAssign.class));

        byte[] resultdata = {'f','o', 'o'};
        WorkResponse workResponse = new WorkComplete(jobHandle[0], resultdata);
        networkManager.workComplete(workResponse, mockWorkerChannel);
        verify(mockClientChannel).write(workResponse);
    }
}
