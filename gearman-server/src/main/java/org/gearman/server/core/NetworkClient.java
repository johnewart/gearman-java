package org.gearman.server.core;

import org.gearman.common.JobStatus;
import org.gearman.common.interfaces.Client;
import org.gearman.common.packets.Packet;
import org.gearman.common.packets.response.*;
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
    public void sendWorkResults(String jobHandle, byte[] data)
    {
        channel.write(new WorkCompleteResponse(jobHandle, data));
    }

    @Override
    public void sendWorkData(String jobHandle, byte[] data) {
        channel.write(new WorkDataResponse(jobHandle, data));
    }

    @Override
    public void sendWorkException(String jobHandle, byte[] exception) {
        channel.write(new WorkExceptionResponse(jobHandle, exception));
    }

    @Override
    public void sendWorkFail(String jobHandle) {
        channel.write(new WorkFailResponse(jobHandle));
    }

    @Override
    public void sendWorkWarning(String jobHandle, byte[] warning) {
        channel.write(new WorkWarningResponse(jobHandle, warning));
    }

    public void send(Packet packet)
    {
        channel.write(packet);
    }

    public void send(WorkResponse packet) {
        channel.write(packet);
    }

    @Override
    public void sendWorkStatus(JobStatus jobStatus) {
        channel.write(new WorkStatus(jobStatus));
    }
}
