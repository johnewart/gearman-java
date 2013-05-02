package org.gearman.server;

import java.util.UUID;

public class JobHandleFactory {

    /**
     * Returns the next available job handle
     * @return
     * 		the next available job handle
     */
    public static final byte[] getNextJobHandle() {
        return UUID.randomUUID().toString().getBytes();
    }
}
