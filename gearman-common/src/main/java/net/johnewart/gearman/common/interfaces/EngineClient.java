package net.johnewart.gearman.common.interfaces;

import net.johnewart.gearman.common.JobStatus;
import net.johnewart.gearman.common.packets.Packet;
import net.johnewart.gearman.common.Job;

/**
 * This interface is for implementations of the clients that are connected to the
 * core engine. (i.e server to client relationship)
 */
public interface EngineClient {
    Job getCurrentJob();
    void setCurrentJob(final Job job);
    void sendWorkResults(final String jobHandle, final byte[] data);
    void sendWorkData(final String jobHandle, final byte[] data);
    void sendWorkException(final String jobHandle, final byte[] exception);
    void sendWorkFail(final String jobHandle);
    void sendWorkWarning(final String jobHandle, final byte[] warning);
    void sendWorkStatus(final JobStatus jobStatus);

    void send(Packet packet);
}

