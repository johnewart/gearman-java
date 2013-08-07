package org.gearman.client;

import org.gearman.net.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class GearmanWorkerPool {
    private int threadCount;
    private final List<Connection> connectionList;
    private final Set<GearmanWorkerThread> threadSet;
    private static Logger LOG = LoggerFactory.getLogger(GearmanWorkerPool.class);

    private GearmanWorkerPool() {
        this.threadCount = 1;
        this.connectionList = new LinkedList();
        this.threadSet = new HashSet();
    }

    private void initialize() {
        for(int i = 0; i < threadCount; i++) {
            GearmanWorker.Builder workerBuilder = new GearmanWorker.Builder();

            for(Connection c : connectionList) {
                workerBuilder.withConnection(new Connection(c));
            }

            threadSet.add(new GearmanWorkerThread(workerBuilder.build()));
        }
    }

    public void doWork() {
        int counter = 0;
        for(GearmanWorkerThread t : threadSet) {
            LOG.debug("Starting worker #" + counter++);
            t.start();
        }

        for(GearmanWorkerThread t : threadSet) {
            try {
                t.join();
            } catch (InterruptedException e) {
                LOG.error("Error waiting for worker thread: ", e);
            }
        }

    }

    public void registerCallback(String functionName, GearmanFunction function) {
        for(GearmanWorkerThread t : threadSet) {
            t.getWorker().registerCallback(functionName, function);
        }
    }

    public static class Builder {
        private GearmanWorkerPool pool;

        public Builder() {
            this.pool = new GearmanWorkerPool();
        }

        public GearmanWorkerPool build() {
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
        private GearmanWorker worker;

        public GearmanWorkerThread(GearmanWorker worker) {
            this.worker = worker;
        }

        public GearmanWorker getWorker() {
            return worker;
        }

        @Override
        public void run() {
            worker.doWork();
        }
    }
}


