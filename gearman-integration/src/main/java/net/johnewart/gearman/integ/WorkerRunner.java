package net.johnewart.gearman.integ;

import net.johnewart.gearman.client.NetworkGearmanWorker;
import net.johnewart.gearman.common.interfaces.GearmanFunction;
import net.johnewart.gearman.common.interfaces.GearmanWorker;
import net.johnewart.gearman.net.Connection;

import java.util.Map;
import java.util.Set;


public class WorkerRunner implements Runnable {
    private GearmanWorker worker;
    private Map<String, GearmanFunction> functions;
    private Set<Connection> connections;

    public WorkerRunner(Set<Connection> connections, Map<String, GearmanFunction> functions) {
        this.functions = functions;
        this.connections = connections;
    }

    @Override
    public void run() {
        NetworkGearmanWorker.Builder builder = new NetworkGearmanWorker.Builder();

        for(Connection c : connections) {
            builder.withConnection(c);
        }

        worker = builder.build();

        for(String function : functions.keySet()) {
            worker.registerCallback(function, functions.get(function));
        }

        worker.doWork();
    }

    public void stop() {
        worker.stopWork();
    }
}
