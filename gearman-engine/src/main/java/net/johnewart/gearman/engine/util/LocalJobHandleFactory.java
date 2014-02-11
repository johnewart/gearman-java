package net.johnewart.gearman.engine.util;

import net.johnewart.gearman.engine.core.JobHandleFactory;

import java.util.concurrent.atomic.AtomicLong;

public class LocalJobHandleFactory implements JobHandleFactory {
    private final AtomicLong jobHandleCounter;
    private final String hostName;

    public LocalJobHandleFactory(String hostName) {
        this.hostName = hostName;
        this.jobHandleCounter = new AtomicLong(0L);
    }
    /**
     * Returns the next available job handle
     * @return
     * 		the next available job handle
     */
    public byte[] getNextJobHandle() {
        String handle = "H:".concat(hostName).concat(":").concat(String.valueOf(jobHandleCounter.incrementAndGet()));
        return handle.getBytes();
    }
}
