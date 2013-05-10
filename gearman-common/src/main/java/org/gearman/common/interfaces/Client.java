package org.gearman.common.interfaces;

import org.gearman.common.packets.Packet;
import org.gearman.common.packets.response.WorkResponse;
import org.gearman.common.Job;

public interface Client {
    public Job getCurrentJob();
    public void setCurrentJob(Job job);
    public void sendJobResults(String jobHandle, byte[] data);
    public void sendJobData(String jobHandle, byte[] data);
    public void sendJobException(String jobHandle, byte[] exception);
    public void send(Packet packet);
    public void send(WorkResponse packet);
}
