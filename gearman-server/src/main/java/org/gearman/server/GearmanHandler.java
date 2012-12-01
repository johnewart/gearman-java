package org.gearman.server;

import org.gearman.common.packets.Packet;
import org.gearman.common.packets.PacketFactory;
import org.gearman.common.packets.request.CanDo;
import org.gearman.common.packets.request.SubmitJob;
import org.gearman.server.persistence.RedisQueue;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 12/1/12
 * Time: 1:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class GearmanHandler extends SimpleChannelUpstreamHandler {

    private static final Logger LOG = LoggerFactory.getLogger(GearmanHandler.class);
    private final AtomicLong transferredBytes = new AtomicLong();
    private final JobStore jobStore;
    private final String hostName;

    public GearmanHandler(JobStore jobStore, String hostName)
    {
        LOG.debug("Creating new handler!");
        this.jobStore = jobStore;
        this.hostName = hostName;
    }

    public long getTransferredBytes() {
        return transferredBytes.get();
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
    {
        // Send back the received message to the remote peer.
        ChannelBuffer channelBuffer = (ChannelBuffer)e.getMessage();
        transferredBytes.addAndGet(channelBuffer.readableBytes());

        byte[] packetBytes = new byte[channelBuffer.readableBytes()];
        channelBuffer.readBytes(packetBytes);
        Channel channel = e.getChannel();

        Packet p = PacketFactory.packetFromBytes(packetBytes);
        handlePacket(p, channel);
    }
    private void handlePacket(Packet packet, Channel channel)
    {
        switch(packet.getType())
        {
            case CAN_DO:
            case CAN_DO_TIMEOUT:
                //jobStore.registerWorker(((CanDo)packet).getFunctionName(), channel);
                return;
            case CANT_DO:
                return;
            case ECHO_REQ:
                return;
            case GET_STATUS:
                return;
            case GRAB_JOB:
                return;
            case GRAB_JOB_UNIQ:
                return;
            case OPTION_REQ:
                return;
            case PRE_SLEEP:
                return;
            case RESET_ABILITIES:
                return;
            case SET_CLIENT_ID:
                return;
            case SUBMIT_JOB:
            case SUBMIT_JOB_BG:
            case SUBMIT_JOB_HIGH:
            case SUBMIT_JOB_HIGH_BG:
            case SUBMIT_JOB_LOW:
            case SUBMIT_JOB_LOW_BG:
                //jobStore.createJob((SubmitJob)packet, channel);
                return;
            case WORK_COMPLETE:
            case WORK_DATA:
            case WORK_WARNING:
            case WORK_EXCEPTION:
            case WORK_FAIL:
            case WORK_STATUS:
                return;

            // Response Only Packets
            case NOOP:
            case JOB_CREATED:
            case NO_JOB:
            case ECHO_RES:
            case ERROR:
            case STATUS_RES:
            case OPTION_RES:
            case JOB_ASSIGN:
            case JOB_ASSIGN_UNIQ:

                // Packets Not Yet Implemented
            case ALL_YOURS:
            case SUBMIT_JOB_EPOCH:
            case SUBMIT_JOB_SCHED:
                //client.sendPacket(StaticPackets.ERROR_BAD_COMMAND, null);
                return;

            // Unknown Command
            default:
                //client.sendPacket(StaticPackets.ERROR_BAD_COMMAND, null);
                return;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
    {
        // Close the connection when an exception is raised.
        LOG.warn("Unexpected exception from downstream.", e.getCause());
        e.getChannel().close();
    }

    /*
    try {
        ServerSocket socket = new ServerSocket(this.port);
        long connectionCount = 0L;

        while (true)
        {
            Socket clientConn = socket.accept();
            LOG.info("Accepted connection #" + connectionCount + " on " + port);
            connectionCount += 1;
            new ServerThread(clientConn, connectionCount, jobStore, hostName).start();
        }
    } catch (IOException ioe) {
        LOG.error("Problem reading from client: ", ioe);
    }
     */
}
