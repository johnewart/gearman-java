package org.gearman.server;

import org.gearman.common.packets.Packet;
import org.gearman.common.packets.request.CanDo;
import org.gearman.common.packets.request.SubmitJob;
import org.gearman.net.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.util.LocaleServiceProviderPool;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 11:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class ServerThread extends Thread {
    private long connectionCount;
    private final Client client;
    private final JobStore jobStore;
    private final String hostName;
    private final Socket socket;
    private final Logger LOG = LoggerFactory.getLogger(ServerThread.class);

    public ServerThread(Socket socket, long connectionCount, JobStore jobStore, String hostName) throws IOException
    {
        this.socket = socket;
        this.client = new Client(socket);
        this.connectionCount = connectionCount;
        this.jobStore = jobStore;
        this.hostName = hostName;
    }

    private void handleConnection() throws SocketException, IOException
    {
        while(true)
        {
            Packet packet = client.getNextPacket();
            if(packet != null)
                handlePacket(packet);
        }
    }

    public final void run() {
        try {
            handleConnection();
        } catch (SocketException se) {
            // Do nothing
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            try {
                socket.close();
            } catch (IOException ioe) {
                LOG.error("Can't close socket: ", ioe);
            }
        }
    }

    private void handlePacket(Packet packet)
    {
        switch(packet.getType())
        {
            case CAN_DO:
            case CAN_DO_TIMEOUT:
                jobStore.registerWorker(((CanDo)packet).getFunctionName(), client);
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
                jobStore.createJob((SubmitJob)packet, client);
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

}
