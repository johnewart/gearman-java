package org.gearman.client;

import org.gearman.common.packets.Packet;
import org.gearman.common.packets.request.GetStatus;
import org.gearman.common.packets.request.SubmitJob;
import org.gearman.common.packets.response.JobCreated;
import org.gearman.common.packets.response.StatusRes;
import org.gearman.common.packets.response.WorkCompleteResponse;
import org.gearman.constants.JobPriority;
import org.gearman.constants.PacketType;
import org.gearman.exceptions.NoServersAvailableException;
import org.gearman.net.Connection;
import org.gearman.net.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class GearmanClient {
    // A list of managers to cycle through
    private final Logger LOG = LoggerFactory.getLogger(GearmanClient.class);
    private final ConnectionPool connectionPool;

    private GearmanClient()
    {
        connectionPool = new ConnectionPool();
    }

    public GearmanClient(String host, int port) throws IOException
    {
        this();
        connectionPool.addHostPort(host, port);
    }

    public GearmanClient(String host) throws IOException
    {
        this(host, 4730);
    }

    public void addHostToList(String host, int port) throws IOException
    {
        connectionPool.addHostPort(host, port);
    }

    public void addHostToList(String host) throws IOException
    {
        this.addHostToList(host, 4730);
    }

    public void close()
    {
        connectionPool.cleanup();
    }

    public ServerResponse sendJobPacket(SubmitJob jobPacket) throws NoServersAvailableException
    {
        Packet result;
        Connection connection;

        while ( (connection = connectionPool.getConnection()) != null)
        {
            try {

                connection.sendPacket(jobPacket);

                System.err.println("Sent job request to " + connection.toString());

                // We need to get back a JOB_CREATED packet
                result = connection.getNextPacket();

                // If we get back a JOB_CREATED packet, we can continue
                // otherwise try the next job manager
                if (result != null && result.getType() == PacketType.JOB_CREATED)
                {
                    System.err.println("Created job " + ((JobCreated) result).getJobHandle());
                    return new ServerResponse(connection, result);
                }
            } catch (IOException ioe) {
                System.err.println("Connection to " + connection.toString() + " flaky, marking as bad.");
            }
        }

        // The only way we get here is if connection == null, by which point we would have been
        // thrown a NoServersAvailableException()
        throw new NoServersAvailableException();
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
                        WorkCompleteResponse wc = (WorkCompleteResponse)result;

                        System.err.println("Completed job " +  wc.getJobHandle());
                        return wc.data;
                    }
                }
            } else {
                System.err.println("Unable to submit job to job severs...");
            }
        } catch (IOException e) {
            System.err.println("Error submitting job: " +  e.toString());
        } catch (NoServersAvailableException nsae) {
            System.err.println("No servers available to submit the job.");
        }

        return null;
    }

    public String submitJobInBackground(String callback, byte[] data, JobPriority priority)
    {

        String jobid = UUID.randomUUID().toString();
        try {
            ServerResponse response = sendJobPacket(new SubmitJob(callback, jobid, data, true, priority));
            if(response != null)
            {

                System.err.println("Sent background job request to " + response.getConnection());

                // If we get back a JOB_CREATED packet, we can continue,
                // otherwise try the next job manager
                if (response.getPacket().getType() == PacketType.JOB_CREATED)
                {
                    String jobHandle =  ((JobCreated)response.getPacket()).getJobHandle();
                    System.err.printf("Created background job %s, with priority %s\n", jobHandle, priority.toString());
                    return jobHandle;
                }
            }
        } catch (NoServersAvailableException nsae) {
            System.err.println("No servers available to submit the job.");
        }

        return null;
    }


    // TODO: Implement a percentage done feedback in the future?

    public StatusRes getStatus(String jobHandle)
    {
        GetStatus statusPkt = new GetStatus(jobHandle);

        Packet result = null;

        for (Connection conn : connectionPool.getGoodConnectionList())
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
                // Do nothing, we don't really care much here.
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