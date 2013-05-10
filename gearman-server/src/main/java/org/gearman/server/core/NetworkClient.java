package org.gearman.server.core;

import org.gearman.common.interfaces.Client;
import org.gearman.common.packets.Packet;
import org.gearman.common.packets.response.WorkComplete;
import org.gearman.common.packets.response.WorkData;
import org.gearman.common.packets.response.WorkException;
import org.gearman.common.packets.response.WorkResponse;
import org.gearman.common.Job;
import org.jboss.netty.channel.Channel;

public class NetworkClient implements Client {
    private final Channel channel;
    private Job currentJob;

    public NetworkClient(Channel channel)
    {
        this.channel = channel;
    }

    @Override
    public Job getCurrentJob() {
        return currentJob;
    }

    @Override
    public void setCurrentJob(Job job) {
        this.currentJob = job;
    }

    @Override
    public void sendJobResults(String jobHandle, byte[] data)
    {
        channel.write(new WorkComplete(jobHandle, data));
    }

    @Override
    public void sendJobData(String jobHandle, byte[] data) {
        channel.write(new WorkData(jobHandle, data));
    }

    @Override
    public void sendJobException(String jobHandle, byte[] exception) {
        channel.write(new WorkException(jobHandle, exception));
    }

    public void send(Packet packet)
    {
        channel.write(packet);
    }

    public void send(WorkResponse packet) {
        channel.write(packet);
    }
}
