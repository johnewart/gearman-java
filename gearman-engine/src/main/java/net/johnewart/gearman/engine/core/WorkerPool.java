package net.johnewart.gearman.engine.core;

import net.johnewart.gearman.common.interfaces.EngineWorker;
import org.eclipse.jetty.util.ConcurrentHashSet;

import java.util.concurrent.atomic.AtomicLong;

public class WorkerPool {
    private final ConcurrentHashSet<EngineWorker> sleepingWorkers;
    private final ConcurrentHashSet<EngineWorker> activeWorkers;
    private final AtomicLong numberOfConnectedWorkers;

    public WorkerPool(final String name) {
        sleepingWorkers = new ConcurrentHashSet<>();
        activeWorkers = new ConcurrentHashSet<>();
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
