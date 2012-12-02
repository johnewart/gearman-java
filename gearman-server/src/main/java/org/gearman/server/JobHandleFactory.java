package org.gearman.server;

import org.gearman.constants.GearmanConstants;
import java.util.concurrent.atomic.AtomicLong;

public class JobHandleFactory {

    /** The prefix for the job handle */
    private static final byte[] jobHandlePrefix = initJobHandle();
    /** The current job handle number */
    private static AtomicLong jobHandleNumber = new AtomicLong(0);

    /**
     * Initializes the jobHandlePrefix variable
     * @return
     * 		The prefix for the job handle
     */
    public static final byte[] initJobHandle() {
        String user;
        try {
            user = java.net.InetAddress.getLocalHost().getHostName();
        } catch (Throwable e) {
            return ("gearman:").getBytes(GearmanConstants.CHARSET);
        }

        return (user + ':').getBytes(GearmanConstants.CHARSET);
    }
    /**
     * Returns the next available job handle
     * @return
     * 		the next available job handle
     */
    public static final byte[] getNextJobHandle() {
        final byte[] jobNumber = Long.toString(jobHandleNumber.incrementAndGet()).getBytes(GearmanConstants.CHARSET);

        final byte[] jobHandle = new byte[jobHandlePrefix.length+jobNumber.length];
        System.arraycopy(jobHandlePrefix, 0, jobHandle, 0, jobHandlePrefix.length);
        System.arraycopy(jobNumber, 0, jobHandle, jobHandlePrefix.length, jobNumber.length);

        return jobHandle;
    }
}
