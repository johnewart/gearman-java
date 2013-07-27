package org.gearman.client;

import org.gearman.net.Connection;

public class GearmanWorker {
    private final Connection connection;

    public GearmanWorker(Connection conn)
    {
        this.connection = conn;
    }
}
