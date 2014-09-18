package net.johnewart.gearman.engine.core;

import net.johnewart.gearman.common.Job;
import net.johnewart.gearman.common.interfaces.EngineClient;
import org.eclipse.jetty.util.ConcurrentHashSet;

import java.util.Map;
import java.util.Set;

public class LocalJobPool implements JobPool {
    private Map<String, Set<EngineClient>> uniqueIdClients;

    @Override
    public void addJob(Job job) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void addClientForUniqueId(String uniqueId, EngineClient client) {
        clientsForUniqueId(uniqueId).add(client);
    }

    @Override
    public Set<EngineClient> clientsForUniqueId(String uniqueId) {
        if(uniqueIdClients.containsKey(uniqueId))
            return uniqueIdClients.get(uniqueId);
        else {
            Set<EngineClient> clients = new ConcurrentHashSet<>();
            uniqueIdClients.put(uniqueId, clients);
            return clients;
        }
    }

    @Override
    public Job getJobByJobHandle(String jobHandle) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Job getJobByUniqueId(String uniqueId) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removeClientForUniqueId(String uniqueId, EngineClient client) {
        clientsForUniqueId(uniqueId).remove(client);
    }
}
