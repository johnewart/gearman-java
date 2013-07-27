package org.gearman.net;

import org.gearman.common.packets.request.EchoRequest;
import org.gearman.common.packets.response.EchoResponse;
import org.gearman.common.packets.response.JobCreated;
import org.gearman.exceptions.NoServersAvailableException;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import static junit.framework.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class ConnectionPoolTest {
    @Test
    public void testReEstablishingConnectionsWorks() {

        Connection connection1 = mock(Connection.class);
        Connection connection2 = mock(Connection.class);

        ConnectionPool connectionPool = new ConnectionPool();
        connectionPool.addConnection(connection1);
        connectionPool.addConnection(connection2);

        try {
            connectionPool.getConnection();
        } catch (NoServersAvailableException nsae) {
            assertNotNull(nsae);
        }

        try {
            // Simulate server coming back
            final EchoResponse echoResponse = new EchoResponse(new EchoRequest("ok"));
            when(connection1.isHealthy()).thenReturn(true);
            when(connection1.getNextPacket()).thenReturn(echoResponse);
            Connection c = connectionPool.getConnection();
            assertEquals(c, connection1);
        } catch (NoServersAvailableException nsae) {
            fail();
        } catch (IOException ioe) {
            fail();
        }
    }
}
