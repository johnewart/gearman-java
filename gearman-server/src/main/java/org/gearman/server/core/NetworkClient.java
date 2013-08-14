package org.gearman.server.core;

import io.netty.channel.Channel;
import org.gearman.common.JobStatus;
import org.gearman.common.interfaces.Client;
import org.gearman.common.packets.Packet;
import org.gearman.common.packets.response.*;
import org.gearman.common.Job;

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
        send(new WorkCompleteResponse(jobHandle, data));
    }

    @Override
    public void sendWorkData(String jobHandle, byte[] data) {
        send(new WorkDataResponse(jobHandle, data));
    }

    @Override
    public void sendWorkException(String jobHandle, byte[] exception) {
        send(new WorkExceptionResponse(jobHandle, exception));
    }

    @Override
    public void sendWorkFail(String jobHandle) {
        send(new WorkFailResponse(jobHandle));
    }

    @Override
    public void sendWorkWarning(String jobHandle, byte[] warning) {
        send(new WorkWarningResponse(jobHandle, warning));
    }

    @Override
    public void sendWorkStatus(JobStatus jobStatus) {
        send(new WorkStatus(jobStatus));
    }

    @Override
    public void send(Packet packet)
    {
        channel.writeAndFlush(packet);
    }

}
