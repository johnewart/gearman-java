package org.gearman.server;

import com.google.common.primitives.Ints;
import org.gearman.common.packets.Packet;
import org.gearman.common.packets.response.JobAssign;
import org.gearman.common.packets.response.JobAssignUniq;
import org.gearman.common.packets.response.JobCreated;
import org.gearman.common.packets.response.WorkStatus;
import org.gearman.constants.GearmanConstants;
import org.gearman.constants.JobPriority;
import org.jboss.netty.channel.Channel;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;


public class Job {

    public static enum JobState {
        QUEUED, WORKING, COMPLETE
    }

    public static enum JobAction {
        REENQUEUE, MARKCOMPLETE, DONOTHING
    }


    private static final byte[] STATUS_TRUE = new byte[]{'1'};
    private static final byte[] STATUS_FALSE = new byte[]{'0'};

    /** Defines this job's current state */
    private JobState state = JobState.QUEUED;
    /** Defines this job's priority. Also used as the state change lock */
    private final JobPriority priority;
    /** Specifies if this is a background or not */
    private final boolean background;

    // --- Job Data --- //

    /** The name of the function */
    private final String functionName;

    /** The function local ID specified by the user */
    private final String uniqueID;
    /** The server wide ID specified by the server */
    private final String jobHandle;
    /** The opaque data that is given as an argument in the SUBMIT_JOB packet*/
    private final byte[] data;
    /** The status numerator */
    private byte[] numerator;
    /** The status denominator */
    private byte[] denominator;
    // Time-stmap of when to run
    private long timeToRun;

    //--- Listening Clients and Worker --- //

    /** The set of all listing clients */
    private final Set<Channel> clients = new CopyOnWriteArraySet<>();
    /** The worker assigned to work on this job */
    private Channel worker;

    public Job()
    {
        background = false;
        functionName = null;
        uniqueID     = null;
        jobHandle    = null;
        data         = null;
        priority     = JobPriority.NORMAL;
        timeToRun    = -1;
    }

    public Job(final String functionName,
               final String uniqueID,
               final byte[] data,
               final JobPriority priority,
               boolean isBackground,
               long timeToRun,
               final Channel creator)
    {
        this(functionName, uniqueID, data, JobHandleFactory.getNextJobHandle(), priority, isBackground, timeToRun, creator);
    }

    public Job(final String functionName,
               final String uniqueID,
               final byte[] data,
               final byte[] jobHandle,
               final JobPriority priority,
               boolean isBackground,
               long timeToRun,
               final Channel creator)
    {
        this.functionName = functionName;
        this.uniqueID = uniqueID;
        this.data = data;
        this.priority = priority;
        this.timeToRun = timeToRun;

        if(!(this.background = isBackground)) {
            this.clients.add(creator);
        }

        this.jobHandle = new String(jobHandle);
    }

    public String getFunctionName() {
        return functionName;
    }

    public final Set<Channel> getClients()
    {
        return clients;
    }

    protected final boolean addClient(final Channel client) {
        return this.clients.add(client);
    }

    public final Packet createJobAssignPacket() {
        return  new JobAssign(jobHandle, this.getFunctionName(), data);
    }

    public final Packet createJobAssignUniqPacket() {
        return new JobAssignUniq(this.jobHandle, this.getFunctionName(), this.uniqueID, data);
    }

    public final Packet createJobCreatedPacket() {
        return new JobCreated(this.jobHandle);
    }

    public final Packet createWorkStatusPacket() {
        return new WorkStatus(this.jobHandle, Ints.fromByteArray(numerator), Ints.fromByteArray(denominator));
    }

    public byte[] getData() {
        return this.data;
    }

    public String getJobHandle() {
        return this.jobHandle;
    }

    public JobPriority getPriority() {
        return this.priority;
    }

    public JobState getState() {
        return this.state;
    }

    public void setState(JobState state)
    {
        this.state = state;
    }

    public long getTimeToRun() {
        return timeToRun;
    }

    public String getUniqueID() {
        return this.uniqueID;
    }

    public boolean isBackground() {
        return this.background;
    }

    public void setStatus(byte[] numerator, byte[] denominator) {
        this.numerator = numerator;
        this.denominator = denominator;
    }


    public final JobAction disconnectClient(final Channel client) {
        JobAction result = JobAction.DONOTHING;

        switch (this.state) {
            case QUEUED:
                assert this.worker==null;
                this.clients.remove(client);
                // If the job was in the QUEUED state, all attached clients have disconnected, and it is not a background job, drop the job
                if(this.clients.isEmpty() && !this.background)
                    result = JobAction.MARKCOMPLETE;
                break;
            case WORKING:
                this.clients.remove(client);
                if(this.worker==client) {
                    this.worker = null;

                    if(this.clients.isEmpty() && !this.background) {
                        // Nobody to send it to and it's not a background job, not much we can do here..
                        result = JobAction.MARKCOMPLETE;
                    } else {
                        // (!this.clients.isEmpty() || this.background)==true
                        result = JobAction.REENQUEUE;
                    }
                }

                /*
                 * If the disconnecting client is not the worker, there is no need to change the state.
                 * Since there is no way to notify the worker that the job is no longer valid, we just
                 * let the worker finish execution
                 */

                break;
            case COMPLETE:
            default:
                result = JobAction.DONOTHING;
                // Do nothing
        }
        if(client==this.worker) {
            this.worker = null;
        }
        this.clients.remove(client);

        return result;
    }

    public final void complete() {
        final JobState prevState = this.state;
        this.state = JobState.COMPLETE;

        this.clients.clear();

        if(this.worker!=null) {
            this.worker = null;
        }

    }

    public void setWorker(Channel worker)
    {
        this.worker = worker;
    }

    public Channel getWorker()
    {
        return worker;
    }

    public String toString()
    {
        return this.getJobHandle();
    }


}