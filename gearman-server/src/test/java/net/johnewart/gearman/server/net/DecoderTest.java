package net.johnewart.gearman.server.net;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import net.johnewart.gearman.server.net.Decoder;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import net.johnewart.gearman.common.packets.Packet;
import net.johnewart.gearman.common.packets.request.EchoRequest;
import net.johnewart.gearman.constants.PacketType;

public class DecoderTest {
    @Test
    public void decodeBinaryPacket() throws Exception {
        Decoder d = spy(new Decoder());
        ByteBuf mockBuffer = mock(ByteBuf.class);
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        List<Object> decodedObjects = new LinkedList<>();

        EchoRequest echoPacket = new EchoRequest("OHAI");
        byte zeroByte = 0;
        final byte[] packetData = echoPacket.toByteArray();

        when(mockBuffer.readByte()).thenReturn(zeroByte);
        when(mockBuffer.readBytes(any(byte[].class), anyInt(), anyInt())).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                byte[] buf = (byte[]) invocationOnMock.getArguments()[0];
                int offset = (Integer) invocationOnMock.getArguments()[1];
                int size = (Integer) invocationOnMock.getArguments()[2];
                for(int i = 0; i < size; i++) {
                    buf[offset+i] = packetData[offset+i];
                }
                return null;
            }
        });

        d.decode(mockCtx, mockBuffer, decodedObjects);

        assertThat(decodedObjects.size(), is(1));
        Packet p = (Packet)decodedObjects.get(0);
        assertThat(p.getType(), is(PacketType.ECHO_REQ));
    }

    @Test
    public void decodeTextPacket() throws Exception {
        Decoder d = spy(new Decoder());
        ByteBuf mockBuffer = mock(ByteBuf.class);
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        List<Object> decodedObjects = new LinkedList<>();

        final byte[] commandMessage = {'S','T','A','T','U','S',10};

        final AtomicInteger byteCounter = new AtomicInteger(0);
        when(mockBuffer.readByte()).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return commandMessage[byteCounter.getAndIncrement()];
            }
        });

        d.decode(mockCtx, mockBuffer, decodedObjects);

        assertThat(decodedObjects.size(), is(1));
        String command = (String)decodedObjects.get(0);
        assertThat(command, is("STATUS"));
    }
}
