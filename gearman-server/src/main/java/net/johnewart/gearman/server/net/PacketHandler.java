package net.johnewart.gearman.server.net;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import net.johnewart.gearman.common.packets.Packet;
import net.johnewart.gearman.common.packets.request.CanDo;
import net.johnewart.gearman.common.packets.request.CanDoTimeout;
import net.johnewart.gearman.common.packets.request.CantDo;
import net.johnewart.gearman.common.packets.request.EchoRequest;
import net.johnewart.gearman.common.packets.request.GetStatus;
import net.johnewart.gearman.common.packets.request.OptionRequest;
import net.johnewart.gearman.common.packets.request.SubmitJob;
import net.johnewart.gearman.common.packets.response.WorkResponse;
import net.johnewart.gearman.common.packets.response.WorkStatus;
import net.johnewart.gearman.engine.queue.JobQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PacketHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger LOG = LoggerFactory.getLogger(PacketHandler.class);
    private final NetworkManager networkManager;

    public PacketHandler(NetworkManager networkManager)
    {
        LOG.debug("Creating new handler!");
        this.networkManager = networkManager;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object request)  throws Exception {
        LOG.debug(" ---> " + request.toString());
        if (request instanceof Packet) {
            handlePacket((Packet)request, ctx.channel());
        } else if (request instanceof String) {
            handleTextCommand((String)request, ctx.channel());
        } else {
            LOG.error("Received un-handled message: " + request);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOG.debug("Client closed channel: " + ctx.channel().toString());
        networkManager.channelDisconnected(ctx.channel());
    }

    private void handleTextCommand(String message, Channel channel)
    {
        switch(message.toLowerCase()) {
            case "status":
                String header = "FUNCTION\tTOTAL\tRUNNING\tAVAILABLE_WORKERS\n";
                Map<String, JobQueue> jobQueues = networkManager.getJobManager().getJobQueues();
                channel.writeAndFlush(header);

                for(String jobQueueName : jobQueues.keySet())
                {
                    JobQueue queue = jobQueues.get(jobQueueName);
                    channel.writeAndFlush(String.format("%s\t%s\t%s\t%s\n", jobQueueName, queue.size(), 0, 0));
                }

                channel.writeAndFlush(".\n");
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
                networkManager.registerAbility(((CanDo)packet).getFunctionName(), channel);
                return;
            case CAN_DO_TIMEOUT:
                // TODO: Support timeout
                networkManager.registerAbility(((CanDoTimeout)packet).getFunctionName(), channel);
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
            case GRAB_JOB_ALL:
                // TODO: Consider support for partitioning and reducing here
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
            case WORK_WARNING:
            case WORK_EXCEPTION:
            case WORK_DATA:
            case WORK_FAIL:
                networkManager.workResponse((WorkResponse) packet, channel);
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
                networkManager.handleEchoRequest((EchoRequest)packet, channel);
                return;

            case OPTION_REQ:
                networkManager.handleOptionRequest((OptionRequest)packet, channel);
                return;

            case RESET_ABILITIES:
                networkManager.resetWorkerAbilities(channel);

            case ALL_YOURS:
            case SUBMIT_JOB_SCHED:
                //client.sendPacket(StaticPackets.ERROR_BAD_COMMAND, null);
                return;

            // Unknown Command
            default:
                //client.sendPacket(StaticPackets.ERROR_BAD_COMMAND, null);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        // Close the connection when an exception is raised.
        LOG.warn("Unexpected exception from downstream.", cause);
        ctx.channel().close();
    }


}
