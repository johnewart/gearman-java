package org.gearman.net;

import com.google.common.primitives.Ints;
import org.gearman.common.packets.Packet;
import org.gearman.constants.PacketType;
import org.gearman.common.packets.response.JobCreated;
import org.gearman.common.packets.response.StatusRes;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 9:16 AM
 * To change this template use File | Settings | File Templates.
 */
public class Connection {
    private Socket socket;
    private String hostname;
    private int port;

    public Connection()
    {	}


    public Connection(String hostname, int port) throws IOException
    {
        this.hostname = hostname;
        this.port = port;

        socket = new Socket(hostname, port);
    }


    public Connection(Socket socket)
    {
        this.socket = socket;
    }


    public void sendPacket(Packet p)
    {
        try {
            socket.getOutputStream().write(p.toByteArray());
        } catch (IOException e) {
            System.err.printf("Unable to send packet: %s\n");
            e.printStackTrace();
        }
    }


    public String toString()
    {
        return String.format("%s:%d", this.hostname, this.port);
    }


    public Packet getNextPacket() throws IOException
    {
        int messagesize = -1;
        int messagetype = -1;

        // Initialize to 12 bytes (header only), and resize later as needed
        byte[] header = new byte[12];
        byte[] packetBytes;

        PacketType packetType;
        InputStream is = socket.getInputStream();
        try {

            int numbytes = is.read(header, 0, 12);

            if(numbytes == 12)
            {
                // Check byte count
                byte[] sizebytes = Arrays.copyOfRange(header, 8, 12);
                byte[] typebytes = Arrays.copyOfRange(header, 4, 8);

                messagesize = Ints.fromByteArray(sizebytes);
                messagetype = Ints.fromByteArray(typebytes);

                if (messagesize > 0)
                {
                    // Grow packet buffer to fit data
                    packetBytes = Arrays.copyOf(header, 12 + messagesize);

                    // Receive the remainder of the message
                    numbytes = is.read(packetBytes, 12, messagesize);
                } else {
                    packetBytes = header;
                }

                packetType = PacketType.fromPacketMagicNumber(messagetype);
                switch(packetType)
                {
                    case JOB_CREATED:
                        return new JobCreated(packetBytes);
                    case WORK_DATA:
                        break;
                    case WORK_WARNING:
                        break;
                    case WORK_STATUS:
                        break;
                    case WORK_COMPLETE:
                        break;
                    case WORK_FAIL:
                        break;
                    case WORK_EXCEPTION:
                        break;

                    case STATUS_RES:
                        return new StatusRes(packetBytes);

                    case OPTION_RES:
                        // TODO Implement option response
                        break;

                    /* Client and worker response packets */
                    case ECHO_RES:
                        // TODO Implement the echo response
                        break;
                    case ERROR:
                        // TODO Implement the error packet
                        break;

                    /* Worker response packets */
                    case NOOP:
                        break;
                    case NO_JOB:
                        break;
                    case JOB_ASSIGN:
                        break;
                    case JOB_ASSIGN_UNIQ:
                        break;

                    /* Worker request packets */
                    case CAN_DO:
                        break;
                    case SET_CLIENT_ID:
                        break;
                    case GRAB_JOB:
                        break;
                    case PRE_SLEEP:
                        break;

                    /* Client request packets */
                    case SUBMIT_JOB:
                        break;
                    case SUBMIT_JOB_BG:
                        break;
                    case SUBMIT_JOB_EPOCH:
                        break;
                    default:
                        System.err.printf("Unhandled type: %s\n", messagetype);
                        return null;
                }
            }
        } catch (Exception e) {
            System.err.printf("Exception reading data: %s\n", e);
            e.printStackTrace();
            return null;
        }

        return null;
    }
}



