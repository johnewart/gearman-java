package org.gearman.common.interfaces;

import org.gearman.common.JobStatus;
import org.gearman.common.packets.Packet;
import org.gearman.common.packets.response.StatusRes;
import org.gearman.common.packets.response.WorkResponse;
import org.gearman.common.Job;

public interface Client {
    public Job getCurrentJob();
    public void setCurrentJob(Job job);
    public void sendWorkResults(String jobHandle, byte[] data);
    public void sendWorkData(String jobHandle, byte[] data);
    public void sendWorkException(String jobHandle, byte[] exception);
    public void sendWorkFail(String jobHandle);
    public void sendWorkWarning(String jobHandle, byte[] warning);
    public void sendWorkStatus(JobStatus jobStatus);

    public void send(Packet packet);
}

