package org.gearman.common.interfaces;

import org.gearman.common.packets.Packet;
import org.gearman.common.packets.response.WorkResponse;
import org.gearman.common.Job;

public interface Client {
    public Job getCurrentJob();
    public void setCurrentJob(Job job);
    public void send(Packet packet);
    public void send(WorkResponse packet);
}
