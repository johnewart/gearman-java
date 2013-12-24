package net.johnewart.gearman.client;

import net.johnewart.gearman.common.interfaces.GearmanFunction;
import net.johnewart.gearman.common.interfaces.GearmanWorker;
import net.johnewart.gearman.net.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A multi-threaded network-connected worker pool.
 */
public class NetworkGearmanWorkerPool implements GearmanWorker {
    private int threadCount;
    private final List<Connection> connectionList;
    private final Set<GearmanWorkerThread> threadSet;
    private final Set<GearmanWorker> workerSet;

    private final AtomicBoolean isActive;

    private static Logger LOG = LoggerFactory.getLogger(NetworkGearmanWorkerPool.class);

    private NetworkGearmanWorkerPool() {
        this.threadCount = 1;
        this.connectionList = new LinkedList<>();
        this.threadSet = new HashSet<>();
        this.workerSet = new HashSet<>();
        this.isActive = new AtomicBoolean(true);
    }

    private void initialize() {
        for(int i = 0; i < threadCount; i++) {
            NetworkGearmanWorker.Builder workerBuilder = new NetworkGearmanWorker.Builder();

            for(Connection c : connectionList) {
                workerBuilder.withConnection(new Connection(c));
            }

            NetworkGearmanWorker worker = workerBuilder.build();
            threadSet.add(new GearmanWorkerThread(worker));
            workerSet.add(worker);
        }
    }

    @Override
    public void doWork() {
        int counter = 0;
        for(GearmanWorkerThread t : threadSet) {
            LOG.debug("Starting worker #" + counter++);
            t.start();
        }

        for(GearmanWorkerThread t : threadSet) {
            try {
                t.join();
                LOG.debug("Thread " + t.getId() + " joined.");
            } catch (InterruptedException e) {
                LOG.error("Error waiting for worker thread: ", e);
            }
        }

    }

    @Override
    public void stopWork() {
        for(GearmanWorker worker : workerSet) {
            worker.stopWork();
        }
    }

    @Override
    public void registerCallback(String functionName, GearmanFunction function) {
        for(GearmanWorkerThread t : threadSet) {
            t.getWorker().registerCallback(functionName, function);
        }
    }

    public static class Builder {
        private NetworkGearmanWorkerPool pool;

        public Builder() {
            this.pool = new NetworkGearmanWorkerPool();
        }

        public NetworkGearmanWorkerPool build() {
            pool.initialize();
            return pool;
        }

        public Builder threads(int value) {
            pool.threadCount = value;
            return this;
        }

        public Builder withConnection(Connection c) {
            pool.connectionList.add(c);
            return this;
        }
    }

    static class GearmanWorkerThread extends Thread {
        private NetworkGearmanWorker worker;

        public GearmanWorkerThread(NetworkGearmanWorker worker) {
            this.worker = worker;
        }

        public NetworkGearmanWorker getWorker() {
            return worker;
        }

        @Override
        public void run() {
            worker.doWork();
        }
    }
}


