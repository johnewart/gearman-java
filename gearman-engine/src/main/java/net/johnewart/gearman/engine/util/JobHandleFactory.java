package net.johnewart.gearman.engine.util;

import java.util.concurrent.atomic.AtomicLong;

public class JobHandleFactory {
    private static AtomicLong jobHandleCounter = new AtomicLong(0L);
    private static String hostName = "";

    /**
     * Returns the next available job handle
     * @return
     * 		the next available job handle
     */
    public static final byte[] getNextJobHandle() {
        String handle = "H:".concat(hostName).concat(":").concat(String.valueOf(jobHandleCounter.incrementAndGet()));
        return handle.getBytes();
    }

    public static void setHostName(String hostName)
    {
        JobHandleFactory.hostName = hostName;
    }
}
