package net.johnewart.gearman.client;

import net.johnewart.gearman.common.packets.response.JobCreated;
import net.johnewart.gearman.exceptions.JobSubmissionException;
import net.johnewart.gearman.exceptions.NoServersAvailableException;
import net.johnewart.gearman.common.packets.Packet;
import net.johnewart.gearman.common.packets.response.WorkCompleteResponse;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.constants.PacketType;
import net.johnewart.gearman.exceptions.WorkException;
import net.johnewart.gearman.net.Connection;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NetworkGearmanClientTest {
    @Test
    public void testClientTriesSecondServer() throws NoServersAvailableException, IOException {
        Connection mockConnection1 = mock(Connection.class);
        Connection mockConnection2 = mock(Connection.class);
        JobCreated mockPacket = mock(JobCreated.class);
        String jobHandle = "mockserver:1";

        doThrow(new IOException("Narf")).when(mockConnection1).sendPacket(any(Packet.class));

        when(mockPacket.getType()).thenReturn(PacketType.JOB_CREATED);
        when(mockPacket.getJobHandle()).thenReturn(jobHandle);
        when(mockConnection1.isHealthy()).thenReturn(false);

        // This is the one that the job is submitted to
        when(mockConnection2.isHealthy()).thenReturn(true);
        when(mockConnection2.getNextPacket()).thenReturn(mockPacket);

        NetworkGearmanClient client = new NetworkGearmanClient();
        client.addConnection(mockConnection1);
        client.addConnection(mockConnection2);


        byte[] data = {'O','K'};
        String responseJobHandle = client.submitJobInBackground("callback", data, JobPriority.HIGH);
        verify(mockConnection2).sendPacket(any(Packet.class));

        assertThat("The job handle is correct",
                responseJobHandle,
                is(jobHandle));
    }

    @Test
    public void testClientHandlesJobResponse() throws JobSubmissionException, WorkException {
        Connection mockConnection = mock(Connection.class);
        NetworkGearmanClient gearmanClient = new NetworkGearmanClient(mockConnection);

        byte[] jobData = {'o','k'};
        final ArrayList<Packet> packets = new ArrayList();
        packets.add(new JobCreated("job_handle:1"));
        packets.add(new WorkCompleteResponse("job_handle:1", jobData));

        try {
            when(mockConnection.isHealthy()).thenReturn(true);
            when(mockConnection.getNextPacket()).thenAnswer(new Answer<Object>() {
                @Override
                public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                    return packets.remove(0);
                }
            });
        } catch (IOException e) {
            fail();
        }

        byte[] jobResults = gearmanClient.submitJob("job_queue", jobData);

        assertTrue(Arrays.equals(jobData, jobResults));
    }

    @Test
    public void testClientFailsWhenBothServersDown() throws Exception
    {
        Connection mockConnection1 = mock(Connection.class);
        Connection mockConnection2 = mock(Connection.class);

        doThrow(new IOException("Server 1 is down")).when(mockConnection1).sendPacket(any(Packet.class));
        doThrow(new IOException("Server 2 is down")).when(mockConnection1).sendPacket(any(Packet.class));

        NetworkGearmanClient client = new NetworkGearmanClient();
        client.addConnection(mockConnection1);
        client.addConnection(mockConnection2);

        try {
            byte[] data = {'O', 'K'};
            String responseJobHandle = client.submitJobInBackground("callback", data, JobPriority.HIGH);
        } catch (NoServersAvailableException nsae) {
            assertNotNull(nsae);
        }
    }

}
