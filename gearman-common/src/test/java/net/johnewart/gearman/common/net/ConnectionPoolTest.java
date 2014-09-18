package net.johnewart.gearman.common.net;

import net.johnewart.gearman.common.packets.request.EchoRequest;
import net.johnewart.gearman.common.packets.response.EchoResponse;
import net.johnewart.gearman.exceptions.NoServersAvailableException;
import net.johnewart.gearman.net.Connection;
import net.johnewart.gearman.net.ConnectionPool;
import org.junit.Test;

import java.io.IOException;

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
