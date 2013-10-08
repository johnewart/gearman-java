package net.johnewart.gearman.client;

import net.johnewart.gearman.common.interfaces.GearmanFunction;
import net.johnewart.gearman.common.interfaces.GearmanWorker;
import net.johnewart.gearman.common.packets.request.CanDo;
import net.johnewart.gearman.net.Connection;
import net.johnewart.gearman.common.packets.Packet;
import net.johnewart.gearman.common.packets.request.GrabJob;
import net.johnewart.gearman.common.packets.request.PreSleep;
import net.johnewart.gearman.common.packets.response.JobAssign;
import net.johnewart.gearman.common.packets.response.JobAssignUniq;
import net.johnewart.gearman.common.packets.response.WorkCompleteResponse;
import net.johnewart.gearman.net.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NetworkGearmanWorker implements GearmanWorker, Runnable {
    private final ConnectionPool connectionPool;
    private final Map<String, GearmanFunction> callbacks;
    private static Logger LOG = LoggerFactory.getLogger(NetworkGearmanWorker.class);

    private NetworkGearmanWorker()
    {
        this.connectionPool = new ConnectionPool();
        this.callbacks = new HashMap<>();
    }

    private NetworkGearmanWorker(NetworkGearmanWorker other)
    {
        this.connectionPool = other.connectionPool;
        this.callbacks = other.callbacks;
    }

    @Override
    public void registerCallback(String method, GearmanFunction function)
    {
        callbacks.put(method, function);
        broadcastAbility(method);
    }

    @Override
    public void doWork()
    {
        while(true) {
            for(Connection c : connectionPool.getGoodConnectionList()) {
                LOG.debug("Trying " + c.toString());
                try {
                    c.sendPacket(new GrabJob());
                    Packet p = c.getNextPacket();
                    byte[] result;
                    switch(p.getType()) {
                        case JOB_ASSIGN:
                            JobAssign jobAssign = (JobAssign)p;
                            result = callbacks.get(jobAssign.getFunctionName()).process(jobAssign.getJob());
                            c.sendPacket(new WorkCompleteResponse(jobAssign.getJobHandle(), result));
                            break;
                        case JOB_ASSIGN_UNIQ:
                            JobAssignUniq jobAssignUniq = (JobAssignUniq)p;
                            result = callbacks.get(jobAssignUniq.getFunctionName()).process(jobAssignUniq.getJob());
                            c.sendPacket(new WorkCompleteResponse(jobAssignUniq.getJobHandle(), result));
                            break;
                        case NO_JOB:
                            LOG.info("Worker sending PRE_SLEEP and sleeping for 30 seconds...");
                            c.sendPacket(new PreSleep());
                            try {
                                Thread.sleep(30 * 1000);
                            } catch (InterruptedException e) {
                                LOG.error("Error sleeping: ", e);
                            }
                            break;
                    }
                } catch (IOException ioe) {

                }
            }
        }

    }

    private void broadcastAbility(String functionName)
    {
        for(Connection c : connectionPool.getGoodConnectionList())
        {
            try {
                c.sendPacket(new CanDo(functionName));
            } catch (IOException e) {
                LOG.error("IO Exception: ", e);
            }
        }
    }

    @Override
    public void run() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public static class Builder {
        private NetworkGearmanWorker worker;

        public Builder() {
            this.worker = new NetworkGearmanWorker();
        }

        public NetworkGearmanWorker build() {
            return new NetworkGearmanWorker(worker);
        }

        public Builder withConnection(Connection c) {
            worker.connectionPool.addConnection(c);
            return this;
        }
    }

}
