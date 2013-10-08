package net.johnewart.gearman.engine.core;

import net.johnewart.gearman.common.interfaces.EngineWorker;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

public class WorkerPool {
    private final HashSet<EngineWorker> sleepingWorkers;
    private final HashSet<EngineWorker> activeWorkers;
    private final AtomicLong numberOfConnectedWorkers;

    public WorkerPool(final String name) {
        sleepingWorkers = new HashSet<>();
        activeWorkers = new HashSet<>();
        numberOfConnectedWorkers = new AtomicLong(0);
    }

    public void addWorker(final EngineWorker worker) {
        activeWorkers.add(worker);
        numberOfConnectedWorkers.incrementAndGet();
    }

    public void removeWorker(final EngineWorker worker) {
        sleepingWorkers.remove(worker);
        activeWorkers.remove(worker);
        numberOfConnectedWorkers.decrementAndGet();
    }

    public void markSleeping(final EngineWorker worker) {
        sleepingWorkers.add(worker);
    }

    public void wakeupWorkers() {
        for (final EngineWorker w : sleepingWorkers) {
            w.wakeUp();
        }
    }

    public void markAwake(final EngineWorker worker) {
        sleepingWorkers.remove(worker);
    }

    public long getNumberOfConnectedWorkers() {
        return numberOfConnectedWorkers.longValue();
    }
}
