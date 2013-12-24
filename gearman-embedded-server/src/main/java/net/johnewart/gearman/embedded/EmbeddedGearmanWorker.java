package net.johnewart.gearman.embedded;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.interfaces.GearmanFunction;
import net.johnewart.gearman.common.interfaces.EngineWorker;
import net.johnewart.gearman.common.interfaces.GearmanWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class EmbeddedGearmanWorker implements EngineWorker, GearmanWorker, Runnable {
    private final Logger LOG = LoggerFactory.getLogger(EmbeddedGearmanWorker.class);

    private final EmbeddedGearmanServer server;
    private final Map<String, GearmanFunction> abilityMap;
    private volatile boolean running;
    private final Object runLock;

    public EmbeddedGearmanWorker(EmbeddedGearmanServer server) {
        this.server = server;
        this.abilityMap = new HashMap<>();
        this.running = true;
        this.runLock = new Object();
    }

    public void registerCallback(String callback, GearmanFunction function) {
        abilityMap.put(callback, function);
        server.registerWorkerAbility(this, callback);
    }

    @Override
    public void doWork() {
        Job nextJob;

        do {
            nextJob = server.getNextJobForWorker(this);

            if(nextJob != null) {
                LOG.debug("Received work to do: " + nextJob.getJobHandle());
                GearmanFunction function  = abilityMap.get(nextJob.getFunctionName());
                final byte[] results = function.process(nextJob);
                server.completeWork(nextJob, results);
            }
        } while (nextJob != null);

        server.markWorkerAsleep(this);
    }

    @Override
    public void stopWork() {
        this.running = false;
        synchronized(runLock) {
            runLock.notify();
        }
    }

    @Override
    public void run() {
        while(running) {
            LOG.debug("Doing work...");
            doWork();
            LOG.debug("Completed all available work, sleeping...");

            synchronized (runLock) {
                try {
                    LOG.debug("Waiting up to 30s for notification of work...");
                    runLock.wait(30000);
                    LOG.debug("Woke up!!");
                } catch (InterruptedException e) {
                    LOG.error("Error waiting on lock: ", e);
                }
            }
        }
        LOG.info("Worker thread completed.");
    }

    @Override
    public Set<String> getAbilities() {
        return abilityMap.keySet();
    }

    @Override
    public void wakeUp() {
        synchronized (runLock) {
            runLock.notify();
        }
    }

}
