package org.gearman.client;

import org.gearman.common.packets.Packet;
import org.gearman.common.packets.request.GetStatus;
import org.gearman.common.packets.request.SubmitJob;
import org.gearman.common.packets.response.JobCreated;
import org.gearman.common.packets.response.StatusRes;
import org.gearman.common.packets.response.WorkComplete;
import org.gearman.constants.JobPriority;
import org.gearman.constants.PacketType;
import org.gearman.net.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: jewart
 * Date: 11/30/12
 * Time: 7:59 AM
 * To change this template use File | Settings | File Templates.
 */
public class GearmanClient {
    // A list of managers to cycle through
    private List<Connection> managers, bogusManagers;
    private final Logger LOG = LoggerFactory.getLogger(GearmanClient.class);

    // Which connection
    private int connectionIndex;

    private GearmanClient()
    {
        managers = new ArrayList<Connection>();
        bogusManagers = new ArrayList<Connection>();
    }

    public GearmanClient(String host, int port) throws IOException
    {
        this();
        Connection c = new Connection(host, port);
        managers.add(c);
    }

    public GearmanClient(String host) throws IOException
    {
        this(host, 4730);
    }

    public void addHostToList(String host, int port) throws IOException
    {
        managers.add(new Connection(host, port));
    }

    public void addHostToList(String host) throws IOException
    {
        this.addHostToList(host, 4730);
    }

    public void close()
    {
        for(Connection c : managers)
        {
            try {
                c.close();
            } catch (IOException ioe) {
                LOG.error("Unable to close connection to: " + c.toString(), ioe);
            }
        }
    }

    public ServerResponse sendJobPacket(SubmitJob jobPacket)
    {
        Packet result = null;
        Connection connection = null;

        while(managers.size() > 0) {
            try{
                // Simple round-robin submission for now
                connection = managers.get(connectionIndex++ % managers.size());

                connection.sendPacket(jobPacket);

                System.err.println("Sent job request to " + connection.toString());

                // We need to get back a JOB_CREATED packet
                result = connection.getNextPacket();

                // If we get back a JOB_CREATED packet, we can continue
                // otherwise try the next job manager
                if (result.getType() == PacketType.JOB_CREATED)
                {
                    System.err.println("Created job " + ((JobCreated) result).getJobHandle());
                    return new ServerResponse(connection, result);
                }
            } catch (IOException ioe) {
                System.err.println("Connection to " + connection.toString() + " flaky, marking bad for now.");
                bogusManagers.add(connection);
                managers.remove(connection);
            }
        }

        return null;
    }

    public byte[] submitJob(String callback, byte[] data)
    {
        Packet result = null;

        try {
            String jobid = UUID.randomUUID().toString();
            ServerResponse response = sendJobPacket(new SubmitJob(callback, jobid, data, false));
            if(response != null)
            {
                // This method handles synchronous requests, so we wait
                // until we get a work complete packet
                while(true) {
                    result = response.getConnection().getNextPacket();

                    if(result.getType() == PacketType.WORK_COMPLETE)
                    {
                        WorkComplete wc = (WorkComplete)result;

                        System.err.println("Completed job " +  wc.getJobHandle());
                        return wc.data;
                    }
                }
            } else {
                System.err.println("Unable to submit job to job severs...");
            }
        } catch (IOException e) {
            System.err.println("Error submitting job: " +  e.toString());
        }

        return null;
    }

    public String submitJobInBackground(String callback, byte[] data, JobPriority priority)
    {

        String jobid = UUID.randomUUID().toString();
        ServerResponse response = sendJobPacket(new SubmitJob(callback, jobid, data, true, priority));
        if(response != null)
        {

            System.err.println("Sent background job request to " + response.getConnection());

            // If we get back a JOB_CREATED packet, we can continue,
            // otherwise try the next job manager
            if (response.getPacket().getType() == PacketType.JOB_CREATED)
            {
                String jobHandle =  ((JobCreated)response.getPacket()).getJobHandle();
                System.err.printf("Created background job %s, with priority %s", jobHandle, priority.toString());
                return jobHandle;
            }
        }


        return null;
    }


    // TODO: Implement a percentage done feedback in the future?

    public StatusRes getStatus(String jobHandle)
    {
        GetStatus statusPkt = new GetStatus(jobHandle);

        Packet result = null;

        for (Connection conn : managers)
        {
            System.err.printf("Checking for status on %s on %s\n", jobHandle, conn);
            try {
                conn.sendPacket(statusPkt);
                result = conn.getNextPacket();

                if(result.getType() == PacketType.STATUS_RES)
                {
                    StatusRes statusResult = (StatusRes)result;

                    if(statusResult.getJobHandle().equals(jobHandle)) {
                        return statusResult;
                    }
                }
            } catch (IOException ioe) {
                System.err.println("Unable to send request to " + conn + ": " + ioe);
            }
        }

        return null;
    }

    class ServerResponse {
        private Connection connection;
        private Packet packet;

        public ServerResponse(Connection connection, Packet packet)
        {
            this.connection = connection;
            this.packet = packet;
        }

        public Connection getConnection() {
            return connection;
        }

        public Packet getPacket() {
            return packet;
        }
    }

}