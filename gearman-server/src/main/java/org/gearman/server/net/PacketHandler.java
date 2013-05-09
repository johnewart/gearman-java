package org.gearman.server.net;

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

import java.util.Set;

public class PacketHandler extends SimpleChannelUpstreamHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PacketHandler.class);
    private final NetworkManager networkManager;
    private final ChannelGroup channelGroup;

    public PacketHandler(NetworkManager networkManager, ChannelGroup channelGroup)
    {
        LOG.debug("Creating new handler!");
        this.networkManager = networkManager;
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
        } else if (e.getMessage() instanceof String) {
            handleTextCommand((String)e.getMessage(), e.getChannel());
        } else {
            LOG.debug("Received un-handled message: " + e.getMessage());
            super.messageReceived(ctx, e);
        }
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
    {
        LOG.debug("Client closed channel: " + e.toString());
        networkManager.channelDisconnected(e.getChannel());
    }

    private void handleTextCommand(String message, Channel channel)
    {
        switch(message.toLowerCase()) {
            case "status":
                String header = "FUNCTION\tTOTAL\tRUNNING\tAVAILABLE_WORKERS\n";
                Set<String> jobQueueNames = networkManager.getJobStore().getJobQueues().keySet();
                channel.write(header);

                for(String jobQueueName : jobQueueNames)
                {
                    channel.write(String.format("%s\t%s\t%s\t%s\n", jobQueueName, networkManager.getJobStore().getJobQueue(jobQueueName).size(), 0, 0));
                }

                channel.write(".\n");
                break;

            case "workers":
                break;

            case "maxqueue":
                break;

            case "shutdown":
                break;

            case "version":
                break;

            default:
                LOG.debug("Unhandled text command: " + message);
        }
    }

    private void handlePacket(Packet packet, Channel channel)
    {
        switch(packet.getType())
        {
            case CAN_DO:
            case CAN_DO_TIMEOUT:
                networkManager.registerAbility(((CanDo)packet).getFunctionName(), channel);
                return;
            case CANT_DO:
                networkManager.unregisterAbility(((CantDo)packet).getFunctionName(), channel);
                return;
            case GRAB_JOB:
                networkManager.nextJobForWorker(channel, false);
                return;
            case GRAB_JOB_UNIQ:
                networkManager.nextJobForWorker(channel, true);
                return;
            case SUBMIT_JOB:
            case SUBMIT_JOB_BG:
            case SUBMIT_JOB_HIGH:
            case SUBMIT_JOB_HIGH_BG:
            case SUBMIT_JOB_LOW:
            case SUBMIT_JOB_LOW_BG:
            case SUBMIT_JOB_EPOCH:
                networkManager.createJob((SubmitJob)packet, channel);
                break;
            case WORK_COMPLETE:
            case WORK_DATA:
            case WORK_WARNING:
            case WORK_EXCEPTION:
            case WORK_FAIL:
                networkManager.workComplete((WorkResponse)packet, channel);
                return;

            case WORK_STATUS:
                networkManager.updateJobStatus((WorkStatus)packet);
                return;

            case GET_STATUS:
                networkManager.checkJobStatus((GetStatus)packet, channel);
                return;

            case SET_CLIENT_ID:
                return;

            case PRE_SLEEP:
                networkManager.sleepingWorker(channel);
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
