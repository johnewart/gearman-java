package net.johnewart.gearman.net;

import net.johnewart.gearman.common.packets.Packet;
import net.johnewart.gearman.common.packets.request.EchoRequest;
import net.johnewart.gearman.common.packets.response.EchoResponse;
import net.johnewart.gearman.constants.PacketType;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConnectionTest {

    @Test
    public void testConnectionThrowsExceptionWithBadSocket()
    {
        Socket bogusSocket = createMockBogusSocket();

        Connection connection = new Connection(bogusSocket);

        try {
            connection.getNextPacket();
        } catch (IOException e) {
            assertNotNull(e);
            assertFalse(connection.isHealthy());
        }
    }

    @Test
    public void testConnectionParsesPacketData()
    {
        Socket goodSocket = createMockSocket();
        Connection connection = new Connection(goodSocket);

        try {
            Packet p = connection.getNextPacket();
            assertThat("The right type of packet was received",
                    p.getType(),
                    is(PacketType.ECHO_RES));
        } catch (IOException e) {
            fail();
        }
    }

    private Socket createMockBogusSocket()
    {
        Socket mockSocket = mock(Socket.class);
        InputStream mockInputStream = mock(InputStream.class);
        OutputStream mockOutputStream = mock(OutputStream.class);

        try {
            when(mockSocket.getInputStream()).thenReturn(mockInputStream);
            when(mockSocket.getOutputStream()).thenReturn(mockOutputStream);

            when(mockSocket.isClosed()).thenReturn(false);

            when(mockInputStream.read(any(byte[].class), any(int.class), any(int.class))).thenReturn(-1);
            doNothing().when(mockOutputStream).write(any(byte[].class));
        } catch (IOException ioe) {

        }

        return mockSocket;
    }

    private Socket createMockSocket()
    {
        Socket mockSocket = mock(Socket.class);
        InputStream mockInputStream = mock(InputStream.class);
        OutputStream mockOutputStream = mock(OutputStream.class);

        try {
            when(mockSocket.getInputStream()).thenReturn(mockInputStream);
            when(mockSocket.getOutputStream()).thenReturn(mockOutputStream);

            when(mockSocket.isClosed()).thenReturn(false);
            final byte[] echoDataBytes = new EchoResponse(new EchoRequest("OK")).toByteArray();

            when(mockInputStream.read(any(byte[].class), any(int.class), any(int.class))).thenAnswer(new Answer() {
                public Object answer(InvocationOnMock invocation) {
                    Object[] args = invocation.getArguments();
                    byte[] dataArray = (byte[]) args[0];
                    int offset = (int) args[1];
                    int numBytes = (int) args[2];
                    for(int i = 0; i < numBytes; i++)
                    {
                        dataArray[i] = echoDataBytes[offset+i] = echoDataBytes[offset+i];
                    }
                    return numBytes;
                }

            });

            doNothing().when(mockOutputStream).write(any(byte[].class));

        } catch (IOException ioe) {

        }

        return mockSocket;
    }
}
