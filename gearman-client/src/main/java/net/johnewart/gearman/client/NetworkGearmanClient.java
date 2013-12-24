package net.johnewart.gearman.client;

import net.johnewart.gearman.common.JobStatus;
import net.johnewart.gearman.common.client.AbstractGearmanClient;
import net.johnewart.gearman.common.packets.Packet;
import net.johnewart.gearman.common.packets.request.GetStatus;
import net.johnewart.gearman.common.packets.request.SubmitJob;
import net.johnewart.gearman.common.packets.response.*;
import net.johnewart.gearman.constants.JobPriority;
import net.johnewart.gearman.constants.PacketType;
import net.johnewart.gearman.exceptions.*;
import net.johnewart.gearman.net.Connection;
import net.johnewart.gearman.net.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

import static java.lang.String.format;

public class NetworkGearmanClient extends AbstractGearmanClient {
    // A list of managers to cycle through
    private final Logger LOG = LoggerFactory.getLogger(NetworkGearmanClient.class);
    private final ConnectionPool connectionPool;

    public NetworkGearmanClient()
    {
        connectionPool = new ConnectionPool();
    }

    public NetworkGearmanClient(String host, int port) throws IOException
    {
        this();
        connectionPool.addHostPort(host, port);
    }

    public NetworkGearmanClient(String host) throws IOException
    {
        this(host, 4730);
    }

    public NetworkGearmanClient(Connection conn)
    {
        this();
        connectionPool.addConnection(conn);
    }

    public void addConnection(Connection conn)
    {
        connectionPool.addConnection(conn);
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

    public byte[] submitJob(String callback, byte[] data, JobPriority priority) throws NoServersAvailableException, WorkException
    {
        Packet result = null;

        try {
            String jobid = UUID.randomUUID().toString();
            ServerResponse response = sendJobPacket(new SubmitJob(callback, jobid, data, false, priority));
            if(response != null)
            {
                // This method handles synchronous requests, so we wait
                // until we get a work complete packet
                while(true) {
                    result = response.getConnection().getNextPacket();

                    switch(result.getType()) {
                        // WORK_COMPLETE -> return the data in the response, all others
                        // are handled by the event listeners.
                        case WORK_COMPLETE:
                            WorkCompleteResponse wc = (WorkCompleteResponse) result;
                            LOG.debug("Completed job " +  wc.getJobHandle());
                            return wc.data;
                        case WORK_EXCEPTION:
                            WorkExceptionResponse wr = (WorkExceptionResponse) result;
                            LOG.debug("Exception for job " + wr.getJobHandle());
                            throw new WorkExceptionException(wr.getJobHandle(), wr.getException());
                        case WORK_FAIL:
                            WorkFailResponse wf = (WorkFailResponse) result;
                            LOG.debug("Job " + wf.getJobHandle() + " failed.");
                            throw new WorkFailException(wf.getJobHandle());
                        case WORK_STATUS:
                            WorkStatus ws = (WorkStatus) result;
                            LOG.debug("Status data for job " + ws.getJobHandle());
                            handleWorkStatus(ws.getJobHandle(), ws.toJobStatus());
                            break;
                        case WORK_DATA:
                            WorkDataResponse wd = (WorkDataResponse) result;
                            LOG.debug("Received data update for job " + wd.getJobHandle());
                            handleWorkData(wd.getJobHandle(), wd.getData());
                            break;
                        case WORK_WARNING:
                            WorkWarningResponse ww = (WorkWarningResponse) result;
                            LOG.debug("Received warning for job " + ww.getJobHandle());
                            handleWorkWarning(ww.getJobHandle(), ww.getData());
                            break;
                        default:
                            LOG.info("Unexpected message: " + result.getType());
                            break;
                    }
                }
            } else {
                LOG.warn("Unable to submit job to job severs...");
            }
        } catch (IOException e) {
            LOG.error("Error submitting job: ",   e);
        } catch (NoServersAvailableException nsae) {
            LOG.error("No servers available to submit the job.");
        }

        return null;
    }

    @Override
    public String submitFutureJob(String callback, byte[] data, Date whenToRun) throws NoServersAvailableException {
        String uniqueID = UUID.randomUUID().toString();
        try {
            ServerResponse response = sendJobPacket(new SubmitJob(callback, uniqueID, data, whenToRun));
            if(response != null)
            {
                LOG.debug("Sent future job request to " + response.getConnection());

                // If we get back a JOB_CREATED packet, we can continue,
                // otherwise try the next job manager
                if (response.getPacket().getType() == PacketType.JOB_CREATED)
                {
                    String jobHandle =  ((JobCreated)response.getPacket()).getJobHandle();
                    LOG.debug("Created future job %s\n", jobHandle);
                    return jobHandle;
                }
            }
        } catch (NoServersAvailableException nsae) {
            LOG.warn("No servers available to submit the job.");
            throw nsae;
        }

        return null;
    }

    @Override
    public String submitJobInBackground(String callback, byte[] data) throws JobSubmissionException {
        return submitJobInBackground(callback, data, JobPriority.NORMAL);
    }

    public String submitJobInBackground(String callback, byte[] data, JobPriority priority) throws NoServersAvailableException
    {

        String jobid = UUID.randomUUID().toString();
        try {
            ServerResponse response = sendJobPacket(new SubmitJob(callback, jobid, data, true, priority));
            if(response != null)
            {

                LOG.debug("Sent background job request to " + response.getConnection());

                // If we get back a JOB_CREATED packet, we can continue,
                // otherwise try the next job manager
                if (response.getPacket().getType() == PacketType.JOB_CREATED)
                {
                    String jobHandle =  ((JobCreated)response.getPacket()).getJobHandle();
                    LOG.debug(format("Created background job %s, with priority %s\n", jobHandle, priority.toString()));
                    return jobHandle;
                }
            }
        } catch (NoServersAvailableException nsae) {
            LOG.warn("No servers available to submit the job.");
            throw nsae;
        }

        return null;
    }

    @Override
    public byte[] submitJob(String callback, byte[] data) throws JobSubmissionException, WorkException {
        return submitJob(callback, data, JobPriority.NORMAL);
    }

    // TODO: Implement a percentage done feedback in the future?
    public JobStatus getStatus(String jobHandle)
    {
        GetStatus statusPkt = new GetStatus(jobHandle);

        Packet result;

        for (Connection conn : connectionPool.getGoodConnectionList())
        {
            LOG.debug("Checking for status on %s on %s\n", jobHandle, conn);
            try {
                conn.sendPacket(statusPkt);
                result = conn.getNextPacket();

                if(result.getType() == PacketType.STATUS_RES)
                {
                    StatusRes statusResult = (StatusRes)result;

                    if(statusResult.getJobHandle().equals(jobHandle)) {
                        return statusResult.toJobStatus();
                    }
                }
            } catch (IOException ioe) {
                // Do nothing, we don't really care much here.
                LOG.error("Unable to send request to " + conn + ": " + ioe);
            }
        }

        return null;
    }

    private ServerResponse sendJobPacket(SubmitJob jobPacket) throws NoServersAvailableException
    {
        Packet result;
        Connection connection;

        while ( (connection = connectionPool.getConnection()) != null)
        {
            try {

                connection.sendPacket(jobPacket);

                LOG.debug("Sent job request to " + connection.toString());

                // We need to get back a JOB_CREATED packet
                result = connection.getNextPacket();

                // If we get back a JOB_CREATED packet, we can continue
                // otherwise try the next job manager
                if (result != null && result.getType() == PacketType.JOB_CREATED)
                {
                    LOG.debug("Created job " + ((JobCreated) result).getJobHandle());
                    return new ServerResponse(connection, result);
                }
            } catch (IOException ioe) {
                LOG.error("Connection to " + connection.toString() + " flaky, marking as bad.");
            }
        }

        // The only way we get here is if connection == null, by which point we would have been
        // thrown a NoServersAvailableException()
        throw new NoServersAvailableException();
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