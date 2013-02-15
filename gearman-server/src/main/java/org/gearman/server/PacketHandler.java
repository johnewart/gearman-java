package org.gearman.server;

import org.gearman.common.packets.Packet;
import org.gearman.common.packets.request.CanDo;
import org.gearman.common.packets.request.CantDo;
import org.gearman.common.packets.request.GetStatus;
import org.gearman.common.packets.request.SubmitJob;
import org.gearman.common.packets.response.WorkResponse;
import org.gearman.common.packets.response.WorkStatus;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 12/1/12
 * Time: 1:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class PacketHandler extends SimpleChannelUpstreamHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PacketHandler.class);
    private final JobStore jobStore;
    private final String hostName;
    private final ChannelGroup channelGroup;

    public PacketHandler(JobStore jobStore, String hostName, ChannelGroup channelGroup)
    {
        LOG.debug("Creating new handler!");
        this.jobStore = jobStore;
        this.hostName = hostName;
        this.channelGroup = channelGroup;
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        this.channelGroup.add(e.getChannel());
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        LOG.debug(" ---> " + e.toString());
        if (e.getMessage() instanceof Packet) {
            handlePacket((Packet)(e.getMessage()), e.getChannel());
        } else {
            super.messageReceived(ctx, e);
        }
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
    {
        LOG.debug("Client closed channel: " + e.toString());
        jobStore.channelDisconnected(e.getChannel());
    }

    private void handlePacket(Packet packet, Channel channel)
    {
        switch(packet.getType())
        {
            case CAN_DO:
            case CAN_DO_TIMEOUT:
                jobStore.registerWorker(((CanDo)packet).getFunctionName(), channel);
                return;
            case CANT_DO:
                jobStore.unregisterWorker(((CantDo)packet).getFunctionName(), channel);
                return;
            case GRAB_JOB:
                jobStore.nextJobForWorker(channel, false);
                return;
            case GRAB_JOB_UNIQ:
                jobStore.nextJobForWorker(channel, true);
                return;
            case SUBMIT_JOB:
            case SUBMIT_JOB_BG:
            case SUBMIT_JOB_HIGH:
            case SUBMIT_JOB_HIGH_BG:
            case SUBMIT_JOB_LOW:
            case SUBMIT_JOB_LOW_BG:
                jobStore.createJob((SubmitJob)packet, channel);
                return;
            case SUBMIT_JOB_EPOCH:
                jobStore.createJob((SubmitJob)packet, channel);
                break;
            case WORK_COMPLETE:
            case WORK_DATA:
            case WORK_WARNING:
            case WORK_EXCEPTION:
            case WORK_FAIL:
                jobStore.workComplete((WorkResponse)packet);
                return;

            case WORK_STATUS:
                jobStore.updateJobStatus((WorkStatus)packet);
                return;

            case GET_STATUS:
                jobStore.checkJobStatus((GetStatus)packet, channel);
                return;

            case SET_CLIENT_ID:
                return;

            case PRE_SLEEP:
                jobStore.sleepingWorker(channel);
                return;

            // Packets Not Yet Implemented
            case ECHO_REQ:
            case OPTION_REQ:
            case RESET_ABILITIES:
            case ALL_YOURS:
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


}
